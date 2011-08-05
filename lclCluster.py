import sys

def possiblePairs(n):
    return n*(n-1) - (((n-1)*n)/2)
    
inOutDegree = {1: 0}
adjNodes = {1: [0,0]}

fileName = sys.argv[1]
if len(sys.argv) < 2:
    print 'One file name must be specified'
    quit()

IFILE = open(fileName, 'r')

line = IFILE.readline().strip()
while line[0] == '#':
    line = IFILE.readline().strip()
    
edge = line.split()
if len(edge) != 2:
    quit()
    
curr = edge[0]
adj = edge[1]
inOutDegree[int(curr)]  = 1
inOutDegree[int(adj)] = 1
adjNodes[int(curr)] = set([int(adj)])
adjNodes[int(adj)] = set([int(curr)])

for line in IFILE:
    line = line.strip()
    edge = line.split()
    if len(edge) != 2:
        continue
    
    curr = edge[0]
    adj = edge[1]
    if curr == '#':
        continue
    
    if int(curr) in inOutDegree:
        inOutDegree[int(curr)] += 1
    else:
        inOutDegree[int(curr)] = 1
            
    if int(adj) in inOutDegree:
        inOutDegree[int(adj)] += 1
    else:
        inOutDegree[int(adj)] = 1
        
    if int(curr) in adjNodes:
        adjNodes[int(curr)].add(int(adj))
    else:
        adjNodes[int(curr)] = set([int(adj)])
    
    if int(adj) in adjNodes:
        adjNodes[int(adj)].add(int(curr))
    else:
        adjNodes[int(adj)] = set([int(curr)])
        
IFILE.close()

outputFile = 'results_' + fileName
OFILE = open(outputFile, 'w')
#OFILE.write(str('node,lcc,gcc\n'))

items = inOutDegree.items()
items.sort()
for key, value in items:
    numAdj = 0
    if key in adjNodes:
        numAdj = len(adjNodes[key])
        
    pairs = possiblePairs(numAdj)
    connectedPairs = 0
    actualEdges = 0
    
    if numAdj > 0:
        adjNodesList = list(adjNodes[key])
        for i in range(0, numAdj-1):
            node1 = adjNodesList[i]
            for j in range(i+1, numAdj):
                node2 = adjNodesList[j]
                if node2 in adjNodes:
                    for node in adjNodes[node2]:
                        if(node == node1):
                            connectedPairs += 1
                            break
                        
        for node1 in adjNodesList:
            for node2 in adjNodesList:
                if node1 == node2:
                    continue
                
                if node2 in adjNodes:
                    for node in adjNodes[node2]:
                        if(node == node1):
                            actualEdges += 1
                            break

    possibleEdges = value*(value-1)
    lcc = 0
    if possibleEdges > 0:
        lcc = float(float(actualEdges) / float(possibleEdges))
        
    gcc = 0
    if pairs > 0:
        gcc = float(connectedPairs)/float(pairs)

    OFILE.write(str(key) + ',' + str(lcc) + ',' + str(gcc) + '\n')

OFILE.close()    
print 'Done'
