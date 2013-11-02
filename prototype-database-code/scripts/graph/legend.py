import matplotlib
from matplotlib.font_manager import FontProperties

from pylab import *
from os import listdir
from sys import argv



fmtdict = {
"CONSTANT_TRANSACTIONREAD_COMMITTED-NO_ATOMICITY" : ["blue", '^-', 'RC'],
"MASTERED_EVENTUAL" : ["teal", 's-', 'Master'],
"CONSTANT_TRANSACTIONREAD_COMMITTED-CLIENT" : ["green", 'o-', 'MAV'],
"EVENTUAL" : ["red", 'x-', 'Eventual']}

order = ["EVENTUAL", "CONSTANT_TRANSACTIONREAD_COMMITTED-NO_ATOMICITY", "CONSTANT_TRANSACTIONREAD_COMMITTED-CLIENT", "MASTERED_EVENTUAL"]



fig = figure()
figlegend = figure(figsize=(2.8, .3))
ax = fig.add_subplot(111)
lines = []
for fmt in order:
    print fmt
    lines.append(ax.plot([0],[0], fmtdict[fmt][1], label=fmtdict[fmt][2], color=fmtdict[fmt][0],  markeredgecolor=fmtdict[fmt][0], markerfacecolor='None', markeredgewidth=1.3, linewidth=1.3))


fontP = FontProperties()
fontP.set_size(8)
fontP.set_weight("normal")
figlegend.legend(lines, [fmtdict[fmt][2] for fmt in order], loc="right", ncol=4, frameon=False, prop=fontP, markerscale=.8)
figlegend.savefig('strategylegend.pdf', transparent=True, pad_inches=.05)
