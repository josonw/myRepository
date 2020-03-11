# -*- coding: utf-8 -*-
"""
    ~~~~~~
    :copyright: (c) 2014 by 数据中心.
"""

from dc_scs import app

# 启动
##

if app.config['DEBUG']:
    app.run(host=app.config['APP_HOST'], port=app.config['APP_PORT'], debug=app.config['DEBUG'])

跟改第一次版本
