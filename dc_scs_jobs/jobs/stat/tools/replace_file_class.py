#-*- coding: UTF-8 -*-
import os
import sys
import re


def modify_py_file(file_path, file_name):
    read_file = open(file_path, "r")

    read_lines = read_file.readlines()
    write_lines = []
    for line in read_lines:
        write_lines.append( line.replace('ChannelData', file_name) )
    read_file.close()

    write_file = open(file_path, "w")
    write_file.writelines(write_lines)

def main():
    dir_path = 'D:\data_oa\\byl_scs\jobs\stat\\user_info'
    file_list = os.listdir(dir_path)
    for file_py in file_list:
        file_name = file_py.split('.')[0]
        file_path = os.path.join(dir_path, file_py)
        modify_py_file(file_path, file_name)

main()
