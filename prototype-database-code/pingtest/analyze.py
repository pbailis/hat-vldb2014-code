
regions = ["virgina",
           "oregon",
           "california",
           "ireland",
           "singapore",
           "tokyo",
           "sydney",
           "saopaulo"]

azs = ["eastb",
       "eastc",
       "eastd"]

eastbs = ["b1",
          "b2",
          "b3"]

sec_per_week=604800

def avg(d):
    return sum(d)/float(len(d))

def get_pctile(d, pct):
    return d[int(pct*len(d))]
    

def fetchTimes(f):
    results = []

    starttime = None

    for line in open(f):
        linesp = line.split()
        if linesp[1] != "64":
            #if linesp[1] != "PING":
                #print line
            continue
        time = int(linesp[0][:-1])
        if starttime is None:
            starttime = time

        results.append(float(linesp[8].split("=")[1]))

        if time-starttime >= sec_per_week:
            results.sort()
            return results

    print "DID NOT GET A WEEK OF DATA!"

def getData(locs):
    for loca in locs:
        for locb in locs:
            if loca == locb:
                continue

            result = fetchTimes("output/%s-%s-times.txt" % (loca, locb))
            print ", ".join([str(s) for s in [loca, locb, min(result), avg(result), get_pctile(result, .5), get_pctile(result, .75), get_pctile(result, .99), get_pctile(result, .999), get_pctile(result, .9999), max(result)]])

print "Site1, Site2, Min, Avg, Median, 75th, 99th, 99.9th, 99.99th, Max"
getData(regions)
getData(azs)
getData(eastbs)
