#!/usr/bin/env python
# coding=utf-8
import sys

print ('Content-Type: text/html\n')
print ('Hello Word from Python receive CGI')

receive = sys.stdin.readline()
receive = receive + "OK!"

print(receive)