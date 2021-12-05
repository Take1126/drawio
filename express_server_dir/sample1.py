import sys
inputPath=sys.stdin.readline().strip()

f = open(inputPath, 'r', encoding='UTF-8')
data = f.read()
print('Hello world from sample1.py')

f.close()