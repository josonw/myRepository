#-*- coding: UTF-8 -*-
import os
import sys
import re


def modify_pck_file(file_path, pro_package_name):
    read_file = open(file_path, "r")

    procedure_pattern = re.compile(r".*procedure +(\w+)", re.I)
    ps_name_pattern = re.compile(r".*ps_name *=> *'(\w+)", re.I)
    ps_subname_pattern = re.compile(r".*ps_subname *=> *'(\w+)", re.I)


    read_lines = read_file.readlines()
    write_lines = []

    procedure_name = ''
    for line in read_lines:

        new_line = line

        procedure_match = procedure_pattern.match( line )
        if procedure_match:
            procedure_name = procedure_match.groups()[0]

        ps_name_match = ps_name_pattern.match( line )
        if ps_name_match:
            ps_name = ps_name_match.groups()[0]
            new_line = line.replace(ps_name, pro_package_name)

        ps_subname_match = ps_subname_pattern.match( line )
        if ps_subname_match:
            ps_subname = ps_subname_match.groups()[0]
            new_line = line.replace(ps_subname, procedure_name)

        write_lines.append( new_line )

    read_file.close()

    write_file = open(file_path, "w")
    write_file.writelines(write_lines)


def parse_pkg_name(folder_path):
    try:
        list_files = os.listdir(folder_path)
    except Exception, e:
        output.write('oops! your url path was wrong, please check.\n')
        return

    for pck_file in list_files:

        if not pck_file.endswith('pck'):
            continue

        pro_package_name = pck_file.split('.pck')[0]
        print pro_package_name

        file_path = os.path.join( folder_path, pck_file)
        print file_path

        modify_pck_file( file_path, pro_package_name )


if __name__ == '__main__':
    folder_path = "D:\data_oa\oracle_proc"
    parse_pkg_name(folder_path)
