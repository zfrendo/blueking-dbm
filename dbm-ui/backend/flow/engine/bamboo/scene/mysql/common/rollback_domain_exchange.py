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

from django.utils.translation import gettext as _

from backend.db_meta.enums import ClusterEntryRole, ClusterEntryType, ClusterType, TenDBClusterSpiderRole
from backend.db_meta.models import Cluster, ClusterEntry
from backend.flow.consts import DnsOpType
from backend.flow.engine.bamboo.scene.common.builder import SubBuilder
from backend.flow.engine.bamboo.scene.common.entrys_manager import BuildDNSManageSubflow
from backend.flow.plugins.components.collections.name_service.mysql_clb_comp import (
    ClbOperationType,
    MySQLClbOperationComponent,
)

logger = logging.getLogger("flow")


def rollback_domain_exchange_subflow(
    root_id: str,
    ticket_data: dict,
    bk_cloud_id: int,
    src_domain: str,
    dst_domain: str,
    src_ips: list[str],
    dst_ips: list[str],
    port: int,
):
    """
    定义域名互换的子流程
    @param root_id: root id
    @param ticket_data: 单据数据
    @param bk_cloud_id: 云区域ID
    @param src_domain: 源域名
    @param dst_domain: 目标域名
    @param src_ips: 源IP列表
    @param dst_ips: 目标IP列表
    """
    # 针对集群维度声明子流程
    exchange_sub_pipeline = SubBuilder(root_id=root_id, data=copy.deepcopy(ticket_data))
    #  先删除双方域名
    domain_del_list = [
        BuildDNSManageSubflow(
            root_id,
            ticket_data,
            DnsOpType.RECYCLE_RECORD,
            {"bk_cloud_id": bk_cloud_id, "port": port, "del_ips": src_ips},
        ),
        BuildDNSManageSubflow(
            root_id,
            ticket_data,
            DnsOpType.RECYCLE_RECORD,
            {"bk_cloud_id": bk_cloud_id, "port": port, "del_ips": dst_ips},
        ),
    ]
    # 并行删除域名子流程
    exchange_sub_pipeline.add_parallel_acts(acts_list=domain_del_list)

    # 互换添加域名
    domain_add_list = [
        BuildDNSManageSubflow(
            root_id,
            ticket_data,
            DnsOpType.CREATE,
            {"bk_cloud_id": bk_cloud_id, "port": port, "add_ips": src_ips, "entry": dst_domain},
        ),
        BuildDNSManageSubflow(
            root_id,
            ticket_data,
            DnsOpType.CREATE,
            {"bk_cloud_id": bk_cloud_id, "port": port, "add_ips": dst_ips, "entry": src_domain},
        ),
    ]
    # 并行添加域名子流程
    exchange_sub_pipeline.add_parallel_acts(acts_list=domain_add_list)

    return exchange_sub_pipeline.build_sub_process(sub_name=_("域名互换"))


def rollback_clb_exchange_subflow(root_id: str, ticket_data: dict, src_cluster: Cluster, dst_cluster: Cluster):
    """
    定义CLB域名互换的子流程
    @param root_id: root id
    @param ticket_data: 单据数据
    @param src_cluster: 源集群
    @param dst_cluster: 目标集群
    """
    exchange_sub_pipeline = SubBuilder(root_id=root_id, data=copy.deepcopy(ticket_data))

    #  先查看是否有dns
    src_master_entrys = ClusterEntry.objects.filter(
        cluster=src_cluster, cluster_entry_type=ClusterEntryType.CLB.value, role=ClusterEntryRole.MASTER_ENTRY
    )
    src_slave_entrys = ClusterEntry.objects.filter(
        cluster=src_cluster, cluster_entry_type=ClusterEntryType.CLB.value, role=ClusterEntryRole.SLAVE_ENTRY
    )
    kwargs_add = {
        "name_service_operation_type": ClbOperationType.CLB_REGISTER_PART_TARGET.value,
        "cluster_id": src_cluster.id,
        "creator": ticket_data["created_by"],
    }
    kwargs_del = {
        "name_service_operation_type": ClbOperationType.CLB_DEREGISTER_PART_TARGET.value,
        "cluster_id": src_cluster.id,
        "creator": ticket_data["created_by"],
    }
    # 删除 clb 注销指向原集群的ip。注册指向目标集群的ip todo 新集群应该是没有clb的吧？
    # 如果不存在clb则直接返回，不生成子流程
    if len(src_master_entrys) == 0 and len(src_slave_entrys) == 0:
        return None
    if src_cluster.cluster_type == ClusterType.TENDBCLUSTER.value:
        if len(src_master_entrys) == 1:
            scr_spider_master_ips = src_cluster.proxyinstance_set.filter(
                tendbclusterspiderext__spider_role=TenDBClusterSpiderRole.SPIDER_MASTER
            )
            dst_spider_master_ips = dst_cluster.proxyinstance_set.filter(
                tendbclusterspiderext__spider_role=TenDBClusterSpiderRole.SPIDER_MASTER
            )
            ips = [i.machine.ip for i in dst_spider_master_ips]
            if len(ips) == 0:
                raise Exception(_("目标集群没有查询到 spider master"))
            kwargs_add["role"] = TenDBClusterSpiderRole.SPIDER_MASTER
            kwargs_add["ips"] = ips
            exchange_sub_pipeline.add_act(
                act_name=_("创建clb master"), act_component_code=MySQLClbOperationComponent.code, kwargs=kwargs_add
            )
            kwargs_del["role"] = TenDBClusterSpiderRole.SPIDER_MASTER
            kwargs_del["ips"] = [i.machine.ip for i in scr_spider_master_ips]
            exchange_sub_pipeline.add_act(
                act_name=_("删除clb master"), act_component_code=MySQLClbOperationComponent.code, kwargs=kwargs_del
            )

        if len(src_slave_entrys) == 1:
            scr_spider_slave_ips = src_cluster.proxyinstance_set.filter(
                tendbclusterspiderext__spider_role=TenDBClusterSpiderRole.SPIDER_SLAVE
            )
            dst_spider_slave_ips = dst_cluster.proxyinstance_set.filter(
                tendbclusterspiderext__spider_role=TenDBClusterSpiderRole.SPIDER_SLAVE
            )
            ips = [i.machine.ip for i in dst_spider_slave_ips]
            if len(ips) == 0:
                raise Exception(_("目标集群没有查询到 spider slave"))
            kwargs_add["ips"] = ips
            kwargs_add["role"] = TenDBClusterSpiderRole.SPIDER_SLAVE
            exchange_sub_pipeline.add_act(
                act_name=_("创建clb slave"), act_component_code=MySQLClbOperationComponent.code, kwargs=kwargs_add
            )
            kwargs_del["role"] = TenDBClusterSpiderRole.SPIDER_SLAVE
            kwargs_del["ips"] = [i.machine.ip for i in scr_spider_slave_ips]
            exchange_sub_pipeline.add_act(
                act_name=_("删除clb slave"), act_component_code=MySQLClbOperationComponent.code, kwargs=kwargs_del
            )

    elif src_cluster.cluster_type == ClusterType.TenDBHA.value:
        src_proxy_ips = src_cluster.proxyinstance_set.filter()
        dst_proxy_ips = dst_cluster.proxyinstance_set.filter()
        ips = [i.machine.ip for i in dst_proxy_ips]
        if len(ips) == 0:
            raise Exception(_("目标集群没有查询到 master proxy"))
        kwargs_add["ips"] = ips
        kwargs_add["role"] = None
        exchange_sub_pipeline.add_act(
            act_name=_("创建clb master proxy"), act_component_code=MySQLClbOperationComponent.code, kwargs=kwargs_add
        )
        kwargs_del["role"] = None
        kwargs_del["ips"] = [i.machine.ip for i in src_proxy_ips]
        exchange_sub_pipeline.add_act(
            act_name=_("删除clb master proxy"), act_component_code=MySQLClbOperationComponent.code, kwargs=kwargs_del
        )

    #  todo 互换完毕修改元数据
    return exchange_sub_pipeline.build_sub_process(sub_name=_("CLB域名互换"))
