#!/Users/takeuchihiroki/.pyenv/versions/anaconda3-5.2.0/envs/Analysis/bin/python
#/usr/bin/env python

# coding=utf-8

# import cgitb
# cgitb.enable()
# import sys
# print ('Content-Type: text/html\n')
# print (sys.version)
import sys,io
import main
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding = 'utf-8')

print('Content-Type: text/html; charset=UTF-8\n')
print ('Hello Word from Python CGI\n')
main.main()

# table=df_product_qty_per_hour.to_html
# # print(table)

# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
# # print('Content-Type: text/html; charset=UTF-8\n')
# html_body = """
# <!DOCTYPE html>
# <html>
# <head>
# </haed>
# <body>
# aaa
# </body>
# </html>
# """

# # form = cgi.FieldStorage()
# # text = form.getvalue('text', '')

# # print(html_body % (df_product_qty_per_hour.to_html))
# print(html_body)

# import cgi
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# # print('Content-Type: text/html; charset=UTF-8\n')
# html_body = """
# <!DOCTYPE html>
# <html>
# <head>
# </haed>
# <body>
# <h1>Your input is "%s"</h1>
# </body>
# </html>
# """

# form = cgi.FieldStorage()
# text = form.getvalue('text', '')

# print(html_body % (text))
# print(df_product_qty_per_hour.to_html())