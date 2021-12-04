# import sys
# message=sys.stdin.readline()
# print(message+'world')

import os
import main
print(os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))
print(os.getcwd())

main.main()