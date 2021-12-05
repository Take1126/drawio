import sys
inputPath=sys.stdin.readline().strip()

f = open(inputPath, 'r', encoding='UTF-8')

data = f.read()
print(data)

f.close()

f = open('./sample3.txt', 'w')
f.write('sample3')  # 何も書き込まなくてファイルは作成されました
f.close()