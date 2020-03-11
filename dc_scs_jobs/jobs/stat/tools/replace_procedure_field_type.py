#-*- coding: UTF-8 -*-
import os
import sys
import re

from analysis_table_field import tables_dict


def modify_py_file(file_path):
    read_file = open(file_path, "r")
    table_pattern = re.compile(r".+create table if not exists analysis\.(\w+)", re.I)
    field_pattern = re.compile(r'.+(f\w+)\s+([\w]+)', re.I)

    # r'\s.*(from|join|table|exists)\s*([a-z._]*)\s.*',re.I

    read_lines = read_file.readlines()
    write_lines = []
    table_name = ''
    for line in read_lines:
        table_match = table_pattern.match( line )
        if table_match:
            table_name = table_match.groups()[0]

        field_match = field_pattern.match( line )
        if field_match:
            field_name, field_type = field_match.groups()
            # print field_name,field_type
            if field_type.lower() in('string', 'bigint'):
                to_type = tables_dict.get(table_name,{}).get(field_name, field_type)

                if field_type == 'string' and to_type.lower() == 'date':
                    write_lines.append(line)
                else:
                    write_lines.append( line.replace(field_type, to_type))
            else:
                write_lines.append(line)
        else:
            write_lines.append(line)

    read_file.close()

    write_file = open(file_path, "w")
    write_file.writelines(write_lines)


def modify_dir_list_file(dir_path):
    file_list = os.listdir(dir_path)
    for file_py in file_list:
        if os.path.isdir(file_py):
            modify_dir_list_file( os.path.join(dir_path, file_py) )
        else:
            file_path = os.path.join(dir_path, file_py)
            modify_py_file( file_path )

def main():
    dir_path = 'D:\data_oa\\byl_scs\jobs\\stat\\channel'
    # modify_py_file(dir_path)
    modify_dir_list_file(dir_path)

main()
