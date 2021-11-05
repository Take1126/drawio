#!/Users/takeuchihiroki/.pyenv/versions/anaconda3-5.2.0/envs/Analysis/bin/python
# -*- coding: utf-8 -*-

import sys

receive = sys.stdin.readline()
receive = receive + "OK!"

print('Content-type: text/html\n')
print(receive)

# def print_receive(receive):
#     print('Content-type: text/html\n')
#     print(receive)
#     return receive

# print_receive()