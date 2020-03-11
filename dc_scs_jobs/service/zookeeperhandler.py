# -*- coding: utf-8 -*-
'''

'''

from kazoo.client import KazooClient
import logging as logger

class ZookeeperHandler(object):


    def __init__(self, cluster_info=None):

        if not cluster_info:
            logger.warning('cluster_info为{}，无法实例化ZookeeperHandler'.format(cluster_info))
            raise Exception('cluster_info must not null')

        self.zookeeper_hosts = cluster_info.get('cluster_zookeeper_host')



    def get(self, path):

        connect = KazooClient(hosts=self.zookeeper_hosts)
        logger.info('zookeeper 获取数据path--{}'.format(path))
        connect.start()
        data = connect.get(path)
        connect.stop()
        connect.close()
        logger.info('zookeeper 获取到的数据--{}'.format(data))
        return data[0]



    def set(self, path, value):

        connect = KazooClient(hosts=self.zookeeper_hosts)
        logger.info('zookeeper 设置数据path:{}--value:{}'.format(path, value))
        connect.start()
        connect.set(path, value)
        connect.stop()
        connect.close()
        logger.info('zookeeper 设置数据成功')
        return True


    def exists(self, path):
        connect = KazooClient(hosts=self.zookeeper_hosts)
        connect.start()
        res = connect.exists(path)
        connect.stop()
        connect.close()
        logger.info('zookeeper 检查路径是否存在path:{}--res:{}'.format(path, res))
        return res

    def create(self, path, value=b""):
        connect = KazooClient(hosts=self.zookeeper_hosts)
        logger.info('zookeeper 创建一个路径path:{}--初值--{}'.format(path, value))
        connect.start()
        connect.create(path, value=value, makepath=True)
        connect.stop()
        connect.close()
        return True

    def delete(self, path):
        connect = KazooClient(hosts=self.zookeeper_hosts)
        logger.info('zookeeper 删除一个路径path:{}'.format(path))
        connect.start()
        connect.delete(path)
        connect.stop()
        connect.close()
        return True


cluster_info = {
        'cluster_zookeeper_host' : '10.30.100.47:2181'
    }

zk = ZookeeperHandler(cluster_info=cluster_info)

if __name__ == '__main__':

    import json


    cluster_info = {
        'cluster_zookeeper_host' : '10.30.100.18:2181,10.30.100.20:2181,10.30.100.21:2181'
    }

    zk = ZookeeperHandler(cluster_info=cluster_info)
    # path = '/businesslog/yjs/schemas/csv/yjs_antibrush'   #256  yjs_antibrush  ok
    # path = '/businesslog/yjs/schemas/csv/yjs_loginlog' #253  yjs_loginlog table not exist
    # path = '/businesslog/yjs/schemas/csv/yjs_missionreward'   #252  ok
    # path = '/businesslog/yjs/schemas/csv/yjs_missionadminlog'   #251  ok
    # path = '/businesslog/yjs/schemas/csv/yjs_iplog'   #250 table not exist
    # path = '/businesslog/yjs/schemas/csv/yjs_test_by_welkinchen'   #249  ok
    #path = '/businesslog/yjs/schemas/csv/yjs_diamondlog'   #247  ok
    # path = '/businesslog/yjs/schemas/csv/yjs_user_login'   #246  ok
    # path = '/businesslog/yjs/schemas/csv/yjs_user_match'   #245  ok




    # path = '/businesslog/yjs/schemas/csv/yjs_crystallog' #248  ok  csv 与 avro字段数量不一致
    # path = '/businesslog/yjs/schemas/avro/yjs_crystallog' #248  fail  #多了一个_date字段，不管这个了，将这个路径删除

    # path = '/businesslog/yjs/schemas/csv/yjs_playlog'  #254 ok  csv与avro字段顺序不一致，但是修改后可以
    # path = '/businesslog/yjs/schemas/avro/yjs_playlog'  #ok


    # path = '/businesslog/yjs/schemas/csv/yjs_moneylog' #255 ok csv与avro字段顺序不一致,但是修改后可以
    # path = '/businesslog/yjs/schemas/avro/yjs_moneylog' #255 ok

    # path = '/businesslog/yjs/schemas/csv/yjs_scorelog' #257 ok
    # path = '/businesslog/yjs/schemas/avro/yjs_scorelog'  #ok

    # path = '/businesslog/yjs/schemas/csv/yjs_gamelog'  #258  ok
    path = '/businesslog/yjs/schemas/avro/yjs_gamelog'  #ok


    business = zk.get(path)
    print business
    # print zk.delete(path)
