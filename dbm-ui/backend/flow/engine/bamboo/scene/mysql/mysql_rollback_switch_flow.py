# -*- coding: utf-8 -*-
"""
TencentBlueKing is pleased to support the open source community by making 蓝鲸智云-DB管理系统(BlueKing-BK-DBM) available.
Copyright (C) 2017-2023 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at https://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""
import copy
import logging.config
from dataclasses import asdict
from typing import Dict, Optional

from django.utils.translation import gettext as _

from backend.db_meta.enums import InstanceInnerRole
from backend.db_meta.models import Cluster
from backend.flow.engine.bamboo.scene.common.builder import Builder, SubBuilder
from backend.flow.engine.bamboo.scene.mysql.clone_grants_from_file import mysql_clone_grants_from_file_subflow
from backend.flow.engine.bamboo.scene.mysql.common.rollback_domain_exchange import rollback_clb_exchange_subflow
from backend.flow.plugins.components.collections.mysql.clone_proxy_client_in_backend import (
    CloneProxyUsersInBackendComponent,
)
from backend.flow.plugins.components.collections.mysql.clone_proxy_user_in_cluster import (
    CloneProxyUsersInClusterComponent,
)
from backend.flow.plugins.components.collections.mysql.mysql_rollback_check_config import (
    MySQLRollbackCheckConfigComponent,
)
from backend.flow.utils.mysql.mysql_act_dataclass import (
    CloneProxyClientInBackendKwargs,
    CloneProxyUsersKwargs,
    ExecuteRdsCompareKwargs,
)
from backend.flow.utils.mysql.mysql_context_dataclass import MySQLFlashBackContext

logger = logging.getLogger("flow")


class MysqlRollbackSwitchFlow(object):
    """
    mysql rollback switch 新机回档关联的切换单据
    """

    def __init__(self, root_id: str, data: Optional[Dict]):
        """
        @param root_id : 任务流程定义的root_id
        @param data : 单据传递过来的参数列表，是dict格式
        """
        self.root_id = root_id
        self.data = data

    def mysql_rollback_switch_flow(self):
        """
        mysql_rollback_switch_flow 集群域名互切
        tendbHa集群间切换：
        1、检查配置项
        2、proxy权限复制
        3、后端db dr权限复制
        4、切换proxy节点映射的域名、切换从节点映射的域名
        5、修改域名相关元数据信息
        """

        cluster_ids = [i["source_cluster_id"] for i in self.data["infos"]]
        dst_cluster_ids = [i["destination_cluster_id"] for i in self.data["infos"]]
        cluster_ids.extend(dst_cluster_ids)
        mysql_rollback_switch_pipeline = Builder(
            root_id=self.root_id, data=copy.deepcopy(self.data), need_random_pass_cluster_ids=list(set(cluster_ids))
        )
        sub_pipeline_list = []
        for info in self.data["infos"]:
            src_cluster = Cluster.objects.get(id=info["source_cluster_id"])
            dst_cluster = Cluster.objects.get(id=info["destination_cluster_id"])

            src_master = src_cluster.storageinstance_set.get(instance_inner_role=InstanceInnerRole.MASTER.value)
            dst_master = dst_cluster.storageinstance_set.get(instance_inner_role=InstanceInnerRole.MASTER.value)
            src_is_standby_slave = src_cluster.storageinstance_set.get(
                instance_inner_role=InstanceInnerRole.SLAVE.value, is_stand_by=True
            )
            dst_is_standby_slave = dst_cluster.storageinstance_set.get(
                instance_inner_role=InstanceInnerRole.SLAVE.value, is_stand_by=True
            )
            # src_slaves = src_cluster.storageinstance_set.filter(instance_inner_role=InstanceInnerRole.SLAVE.value)
            # dst_slaves = dst_cluster.storageinstance_set.filter(instance_inner_role=InstanceInnerRole.SLAVE.value)
            all_dst_instances = dst_cluster.storageinstance_set.all()
            src_proxys = src_cluster.proxyinstance_set.all()
            dst_proxys = dst_cluster.proxyinstance_set.all()

            ticket_data = {
                # **copy.deepcopy(info),
                "cluster_id": src_cluster.id,
                "uid": self.data["uid"],
                "created_by": self.data["created_by"],
                "bk_biz_id": src_cluster.bk_biz_id,
                "bk_cloud_id": src_cluster.bk_cloud_id,
                "module": src_cluster.db_module_id,
                "db_module_id": src_cluster.db_module_id,
                # "ip": master.machine.ip,
                # "port": master.port,
                "ticket_type": self.data["ticket_type"],
            }
            sub_pipeline = SubBuilder(root_id=self.root_id, data=copy.deepcopy(ticket_data))

            # 步骤1 检查配置项: 以源集群 master 为基准，并发对比所有节点 todo 是否要区分 是否可以不检查？
            check_config_acts = []
            for node in all_dst_instances:
                check_config_acts.append(
                    {
                        "act_name": _("检查配置 {} vs {}").format(src_master.ip_port, node.ip_port),
                        "act_component_code": MySQLRollbackCheckConfigComponent.code,
                        "kwargs": asdict(
                            ExecuteRdsCompareKwargs(
                                bk_cloud_id=src_cluster.bk_cloud_id,
                                instance_ip=src_master.machine.ip,
                                instance_port=src_master.port,
                                dst_instance_ip=node.machine.ip,
                                dst_instance_port=node.port,
                            )
                        ),
                    }
                )
            sub_pipeline.add_parallel_acts(acts_list=check_config_acts)

            # 步骤2 复制权限
            #  存储节点 is_stand_by 主从对复制权限
            sub_pipeline.add_parallel_sub_pipeline(
                sub_flow_list=[
                    mysql_clone_grants_from_file_subflow(
                        root_id=self.root_id,
                        data=copy.deepcopy(ticket_data),
                        bk_cloud_id=src_cluster.bk_cloud_id,
                        bk_biz_id=src_cluster.bk_biz_id,
                        source_address=src_master.ip_port,
                        dest_addresses=[dst_master.ip_port],
                    ),
                    mysql_clone_grants_from_file_subflow(
                        root_id=self.root_id,
                        data=copy.deepcopy(ticket_data),
                        bk_cloud_id=src_cluster.bk_cloud_id,
                        bk_biz_id=src_cluster.bk_biz_id,
                        source_address=src_is_standby_slave.ip_port,
                        dest_addresses=[dst_is_standby_slave.ip_port],
                    ),
                ]
            )
            #  todo 其他非 is_standby_by 节点怎么找对应关系呢？

            # 从集群内已有 proxy 克隆用户白名单到新 proxy
            clone_user_acts_list = []
            add_proxy_user_acts_list = []
            for dst_proxy in dst_proxys:
                #  proxy 克隆报名单
                clone_user_acts_list.append(
                    {
                        "act_name": _("克隆proxy用户白名单[{}:{}]".format(dst_proxy.machine.ip, dst_proxy.port)),
                        "act_component_code": CloneProxyUsersInClusterComponent.code,
                        "kwargs": asdict(
                            CloneProxyUsersKwargs(
                                cluster_id=src_cluster.id,
                                target_proxy_host=dst_proxy.machine.ip,
                            )
                        ),
                    }
                )
                #  todo 在后端 MySQL 上为新 proxy 的 IP 创建访问权限 确定是否可跨集群 。需要考虑从所有src_proxy复制吗？
                add_proxy_user_acts_list.append(
                    {
                        "act_name": _("集群对新的proxy添加权限[{}:{}]".format(dst_proxy.machine.ip, dst_proxy.port)),
                        "act_component_code": CloneProxyUsersInBackendComponent.code,
                        "kwargs": asdict(
                            CloneProxyClientInBackendKwargs(
                                cluster_id=dst_cluster.id,
                                target_proxy_host=dst_proxy.machine.ip,
                                origin_proxy_host=src_proxys[0].machine.ip,
                            )
                        ),
                    }
                )

            #  步骤3 域名互切、包括clb域名、proxy 域名。从库域名

            exchange_clb_subflow = rollback_clb_exchange_subflow(
                root_id=self.root_id,
                ticket_data=copy.deepcopy(ticket_data),
                src_cluster=src_cluster,
                dst_cluster=dst_cluster,
            )
            if exchange_clb_subflow is not None:
                sub_pipeline.add_sub_pipeline(sub_flow=exchange_clb_subflow)

            #  修改元数据

            # 从节点域名指向 todo
            #  tendbHa 只有主节点可绑定clb
            # tendbCluster spider master 和slave可绑定clb
            # 现在是根据数据类型查询绑定关系

            # src_cluster.clusterentry_set.get(entry_type=ClusterEntryType.DNS.value).entry_value
            sub_pipeline_list.append(sub_pipeline.build_sub_process(sub_name=_("新机回档集群间互切")))
        mysql_rollback_switch_pipeline.add_parallel_sub_pipeline(sub_flow_list=sub_pipeline_list)
        mysql_rollback_switch_pipeline.run_pipeline(
            init_trans_data_class=MySQLFlashBackContext(), is_drop_random_user=True
        )
