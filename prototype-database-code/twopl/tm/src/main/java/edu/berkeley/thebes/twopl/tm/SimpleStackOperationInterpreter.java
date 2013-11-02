package edu.berkeley.thebes.twopl.tm;

import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.twopl.tm.SimpleStackOperationInterpreter.StatementNode;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class SimpleStackOperationInterpreter implements TwoPLOperationInterpreter<StatementNode> {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(SimpleStackOperationInterpreter.class);

    private IThebesClient client;
    private Map<String, ByteBuffer> mostRecentValues = Maps.newHashMap();
    
    public enum Function {
        PUT (2, "put", false),
        GET (1, "get", false),
        PLUS (2, "+", true),
        MINUS(2, "-", true),
        TIMES(2, "*", true),
        DIVIDE(2, "/", true);
        
        private int numArgs;
        private String name;
        private boolean isExpression;

        private Function(int numArgs, String name, boolean isExpression) {
            this.numArgs = numArgs;
            this.name = name;
            this.isExpression = isExpression;
        }
        
        /** Returns null if this is not a function. */
        public static Function getFunctionByName(String name) {
            for (Function f : Function.values()) {
                if (f.name.equals(name)) {
                    return f;
                }
            }
            return null;
        }
    }
    
    private interface Node {
        public Object evaluate();
    }
    
    private class VariableNode implements Node {
        private String variableName;

        public VariableNode(String variableName){
            this.variableName = variableName;
        }

        @Override
        public Object evaluate() {
            return this;
        }
        
        public String getVariableName() {
            return variableName;
        }
        
        public ByteBuffer resolveValue() {
            return mostRecentValues.get(variableName);
        }
    }
    
    private class ConstantNode implements Node {
        private int constant;

        public ConstantNode(int constant) {
            this.constant = constant;
        }

        @Override
        public Object evaluate() {
            return constant;
        }
    }
    
    private abstract class ExecutableNode implements Node {
        protected Function func;
        protected List<Node> children;
        
        public ExecutableNode(Function func) {
            this.func = func;
            this.children = Lists.newArrayList();
        }
        
        public void addChild(Node child) {
            this.children.add(child);
        }
        
        /** Returns true if this function has all its arguments. */
        public boolean isCompleted() {
            return children.size() == func.numArgs;
        }
    }

    /** Statement nodes cannot self-evaluate, they can only evaluate their sub-expressions. */
    public class StatementNode extends ExecutableNode {
        private List<Object> evaluatedChildren;
        
        public StatementNode(Function func) {
            super(func);
            assert !func.isExpression;
        }
        
        /** Evaluates sub-expressions and returns null. */
        @Override
        public Object evaluate() {
            if (evaluatedChildren != null) {
                return null;
            }

            this.evaluatedChildren = Lists.newArrayList();
            for (Node child : children) {
                evaluatedChildren.add(child.evaluate());
            }
            return null;
        }
        
        public Object getChild(int index) {
            return evaluatedChildren.get(index);
        }
        
        public Function getFunction() {
            return func;
        }
        
        /** For PUT and GET requests, returns the name of the key. */
        public String getTarget() {
            evaluate();
            assert getChild(0) instanceof VariableNode;
            VariableNode target = (VariableNode) getChild(0);
            return target.getVariableName();
        }
    }
    
    /**
     * Expressions can be totally evaluated by the internal interpreter...
     * i.e., they represent only computational logic
     */
    private class ExpressionNode extends ExecutableNode {
        public ExpressionNode(Function func) {
            super(func);
            assert func.isExpression;
        }
        
        private Object evalChild(int index) {
            return children.get(index).evaluate();
        }
        
        private int toInt(Object o) {
            if (o instanceof Integer) {
                return (Integer) o;
            } else if (o instanceof VariableNode) {
                // Force resolution of variables
                VariableNode varNode = (VariableNode) o;
                ByteBuffer bb = varNode.resolveValue();
                assert bb != null : "Attempted to read unknown value: `" + varNode.getVariableName() + "`";
                return bb.getInt(0);
            } else {
                throw new AssertionError("Cannot convert object to int: " + o);
            }
        }

        @Override
        public Object evaluate() {
            switch (func) {
            case DIVIDE:
                return toInt(evalChild(0)) / toInt(evalChild(1)); 
            case MINUS:
                return toInt(evalChild(0)) - toInt(evalChild(1)); 
            case PLUS:
                return toInt(evalChild(0)) + toInt(evalChild(1)); 
            case TIMES:
                return toInt(evalChild(0)) * toInt(evalChild(1)); 
            default:
                throw new IllegalStateException("Don't know how to evaluate func: " + func);
            }
        }
    }
    
    public SimpleStackOperationInterpreter(IThebesClient client) {
        this.client = client;
    }
    
    @Override
    public Map<String, ByteBuffer> getOutput() {
        return mostRecentValues;
    }
    
    @Override
    public StatementNode parse(String operation) {
        String[] tokens = tokenize(operation);
        return parse(tokens);
    }
    
    /** Parses and executes the given operation! 
     * @throws TException */
    @Override
    public ByteBuffer execute(StatementNode operation) throws TException {
        return interpret(operation);
    }
    
    private ByteBuffer interpret(StatementNode node) throws TException {
        String key;
        ByteBuffer value;
        
        node.evaluate();
        switch (node.func) {
        case GET:
            assert node.getChild(0) instanceof VariableNode;
            key = ((VariableNode) node.getChild(0)).getVariableName();
            value = client.get(key);
            if (value != null) {
                mostRecentValues.put(key, value);
                logger.debug("GET " + key + " -> " + value.getInt(0));
            } else {
                logger.debug("GET " + key + " -> " + value);
            }
            return value;
        case PUT:
            assert node.getChild(0) instanceof VariableNode;
            key = ((VariableNode) node.getChild(0)).getVariableName();
            value = toByteBuffer(node.getChild(1));
            assert value != null : "`" + key + "` not found!";
            logger.debug("PUT " + key + " -> " + value.getInt(0));
            client.put(key, value);
            mostRecentValues.put(key, value);
            return value;
        default:
            assert node.getChild(0) instanceof VariableNode;
            throw new AssertionError("Invalid statement: " + node.func);
        }
    }
    
    private ByteBuffer toByteBuffer(Object value) {
        if (value instanceof ByteBuffer) {
            return (ByteBuffer) value;
        } else if (value instanceof VariableNode) {
            return ((VariableNode) value).resolveValue();
        } else if (value instanceof Integer) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt((Integer) value);
            bb.rewind();
            return bb;
        } else {
            throw new AssertionError("Could not evaluate to ByteBuffer: " + value + " [" + value.getClass() + "]");
        }
    }

    private StatementNode parse(String[] tokens) {
        Stack<ExecutableNode> nodeStack = new Stack<ExecutableNode>();
        
        for (int i = 0; i < tokens.length; i ++) {
            String token = tokens[i];
            Function func = Function.getFunctionByName(token);
            if (func != null) {
                ExecutableNode newNode;
                if (func.isExpression) {
                    assert !nodeStack.isEmpty() : "Expression added before statement: " + concatenate(tokens);
                    newNode = new ExpressionNode(func);
                    nodeStack.peek().addChild(newNode);
                } else {
                    assert nodeStack.isEmpty() : "Cannot have multiple statements: " + concatenate(tokens);
                    newNode = new StatementNode(func);
                }
                nodeStack.push(newNode);
            } else {
                // Not a function, interpret it as a variable or number
                Node newNode;
                if (token.matches("[a-zA-Z_][a-zA-Z_0-9]*")) {
                    newNode = new VariableNode(token);
                } else if (token.matches("[0-9]+")) {
                    newNode = new ConstantNode(Integer.parseInt(token));
                } else {
                    throw new AssertionError("Illegal token: `" + token + "` in line `"
                            + concatenate(tokens) + "`");
                }

                assert !nodeStack.isEmpty() : "No function wants this parameter: `" + token
                    + "` in line `" + concatenate(tokens) + "`";
                nodeStack.peek().addChild(newNode);
            }
            
            // Pop the last func off the stack if it's complete
            while (nodeStack.peek().isCompleted()) {
                ExecutableNode lastNode = nodeStack.pop();
                if (nodeStack.isEmpty()) {
                    assert tokens.length == i+1 : "Command malformed, ends at bar: "
                            + concatenate(tokens, 0, i+1) + " | "
                            + concatenate(tokens, i+1, tokens.length);
                    assert lastNode instanceof StatementNode; // This should never fail, checked earlier.
                    return (StatementNode) lastNode;
                }
            }
        }
        
        throw new AssertionError("Command malformed, does not end: " + concatenate(tokens));
    }
    
    private String[] tokenize(String operation) {
        String[] tokens = operation.split(" ");
        for (int i = 0; i < tokens.length; i ++) {
            tokens[i] = tokens[i].trim();
        }
        return tokens;
    }
    
    private String concatenate(String[] tokens) {
        return concatenate(tokens, 0, tokens.length);
    }
    
    private String concatenate(String[] tokens, int start, int end) {
        String out = "";
        for (int i = start; i < end; i ++) {
            out += tokens[i] + " ";
        }
        return out.substring(0, out.length()-1);
    }
}
