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

from backend.db_meta.enums import (
    ClusterEntryRole,
    ClusterEntryType,
    ClusterType,
    InstanceInnerRole,
    TenDBClusterSpiderRole,
)
from backend.db_meta.models import Cluster, ClusterEntry
from backend.flow.consts import DnsOpType
from backend.flow.engine.bamboo.scene.common.builder import SubBuilder
from backend.flow.engine.bamboo.scene.common.entrys_manager import BuildDNSManageSubflow
from backend.flow.plugins.components.collections.name_service.mysql_clb_comp import (
    ClbOperationType,
    MySQLClbOperationComponent,
)

logger = logging.getLogger("flow")


def rollback_domain_exchange_subflow(root_id: str, ticket_data: dict, src_cluster: Cluster, dst_cluster: Cluster):
    """
    定义域名互换的子流程 先删除域名再添加域名
    @param root_id: root id
    @param ticket_data: 单据数据
    @param src_cluster: 源集群
    @param dst_cluster: 目标集群
    """
    exchange_sub_pipeline = SubBuilder(root_id=root_id, data=copy.deepcopy(ticket_data))
    #  查询从节点域名
    if src_cluster.cluster_type == ClusterType.TENDBCLUSTER.value:
        src_master_entry = src_cluster.clusterentry_set.get(
            cluster_entry_type=ClusterEntryType.DNS.value, role=ClusterEntryRole.MASTER_ENTRY.value
        )
        dst_master_entry = dst_cluster.clusterentry_set.get(
            cluster_entry_type=ClusterEntryType.DNS.value, role=ClusterEntryRole.MASTER_ENTRY.value
        )
        src_master_spiders = src_cluster.proxyinstance_set.filter(
            tendbclusterspiderext__spider_role=TenDBClusterSpiderRole.SPIDER_MASTER
        )
        dst_master_spiders = dst_cluster.proxyinstance_set.filter(
            tendbclusterspiderext__spider_role=TenDBClusterSpiderRole.SPIDER_MASTER
        )
        src_master_spiders_ips = [spider.machine.ip for spider in src_master_spiders]
        dst_master_spiders_ips = [spider.machine.ip for spider in dst_master_spiders]
        src_slave_entrys = src_cluster.clusterentry_set.filter(
            cluster_entry_type=ClusterEntryType.DNS.value, role=ClusterEntryRole.SLAVE_ENTRY.value
        )
        dst_slave_entrys = dst_cluster.clusterentry_set.filter(
            cluster_entry_type=ClusterEntryType.DNS.value, role=ClusterEntryRole.SLAVE_ENTRY.value
        )
        src_slave_spiders = src_cluster.proxyinstance_set.filter(
            tendbclusterspiderext__spider_role=TenDBClusterSpiderRole.SPIDER_SLAVE
        )
        dst_slave_spiders = dst_cluster.proxyinstance_set.filter(
            tendbclusterspiderext__spider_role=TenDBClusterSpiderRole.SPIDER_SLAVE
        )
        src_slave_spiders_ips = [spider.machine.ip for spider in src_slave_spiders]
        dst_slave_spiders_ips = [spider.machine.ip for spider in dst_slave_spiders]
        domain_del_list = [
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.RECYCLE_RECORD,
                {
                    "bk_cloud_id": src_cluster.bk_cloud_id,
                    "port": src_master_spiders[0].port,
                    "del_ips": src_master_spiders_ips,
                },
            ),
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.RECYCLE_RECORD,
                {
                    "bk_cloud_id": dst_cluster.bk_cloud_id,
                    "port": dst_master_spiders[0].port,
                    "del_ips": dst_master_spiders_ips,
                },
            ),
        ]

        domain_add_list = [
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.CREATE,
                {
                    "bk_cloud_id": src_cluster.bk_cloud_id,
                    "port": src_master_spiders[0].port,
                    "add_ips": src_master_spiders_ips,
                    "entry": dst_master_entry.entry,
                },
            ),
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.CREATE,
                {
                    "bk_cloud_id": dst_cluster.bk_cloud_id,
                    "port": dst_master_spiders[0].port,
                    "add_ips": dst_master_spiders_ips,
                    "entry": src_master_entry.entry,
                },
            ),
        ]

        if len(src_slave_entrys) == 1:

            if len(src_slave_spiders) > 0:
                #  先删除双方域名
                domain_del_list.append(
                    BuildDNSManageSubflow(
                        root_id,
                        ticket_data,
                        DnsOpType.RECYCLE_RECORD,
                        {
                            "bk_cloud_id": src_cluster.bk_cloud_id,
                            "port": src_slave_spiders[0].port,
                            "del_ips": src_slave_spiders_ips,
                        },
                    )
                )
            if len(dst_slave_spiders) > 0:
                domain_add_list.append(
                    BuildDNSManageSubflow(
                        root_id,
                        ticket_data,
                        DnsOpType.CREATE,
                        {
                            "bk_cloud_id": dst_cluster.bk_cloud_id,
                            "port": dst_slave_spiders[0].port,
                            "add_ips": dst_slave_spiders_ips,
                            "entry": src_slave_entrys[0].entry,
                        },
                    )
                )
        elif len(src_slave_entrys) > 1:
            raise Exception(_("源端查询到多个 spider slave 域名"))

        if len(dst_slave_entrys) == 1:
            if len(dst_slave_spiders) > 0:
                domain_del_list.append(
                    BuildDNSManageSubflow(
                        root_id,
                        ticket_data,
                        DnsOpType.RECYCLE_RECORD,
                        {
                            "bk_cloud_id": dst_cluster.bk_cloud_id,
                            "port": dst_slave_spiders[0].port,
                            "del_ips": dst_slave_spiders_ips,
                        },
                    )
                )
            if len(src_slave_spiders) > 0:
                domain_add_list.append(
                    BuildDNSManageSubflow(
                        root_id,
                        ticket_data,
                        DnsOpType.CREATE,
                        {
                            "bk_cloud_id": src_cluster.bk_cloud_id,
                            "port": src_slave_spiders[0].port,
                            "add_ips": src_slave_spiders_ips,
                            "entry": dst_slave_entrys[0].entry,
                        },
                    )
                )
        elif len(dst_slave_entrys) > 1:
            raise Exception(_("目标端查询到多个 spider slave 域名"))

        exchange_sub_pipeline.add_parallel_acts(acts_list=domain_del_list)
        exchange_sub_pipeline.add_parallel_acts(acts_list=domain_add_list)
    elif src_cluster.cluster_type == ClusterType.TenDBHA.value:
        src_master_entry = src_cluster.clusterentry_set.get(
            cluster_entry_type=ClusterEntryType.DNS.value, role=ClusterEntryRole.MASTER_ENTRY.value
        )
        dst_master_entry = dst_cluster.clusterentry_set.get(
            cluster_entry_type=ClusterEntryType.DNS.value, role=ClusterEntryRole.SLAVE_ENTRY.value
        )
        src_master_proxys = src_cluster.proxyinstance_set.filter()
        dst_master_proxys = dst_cluster.proxyinstance_set.filter()
        src_master_proxy_ips = [proxy.machine.ip for proxy in src_master_proxys]
        dst_master_proxy_ips = [proxy.machine.ip for proxy in dst_master_proxys]

        src_is_standby_slave = src_cluster.storageinstance_set.get(
            instance_inner_role=InstanceInnerRole.SLAVE.value, is_stand_by=True
        )
        dst_is_standby_slave = dst_cluster.storageinstance_set.get(
            instance_inner_role=InstanceInnerRole.SLAVE.value, is_stand_by=True
        )
        src_is_standby_entry = src_is_standby_slave.bind_entry.get(
            cluster_entry_type=ClusterEntryType.DNS.value, role=ClusterEntryRole.SLAVE_ENTRY.value
        )
        dst_is_standby_entry = dst_is_standby_slave.bind_entry.get(
            cluster_entry_type=ClusterEntryType.DNS.value, role=ClusterEntryRole.SLAVE_ENTRY.value
        )
        domain_del_list = [
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.RECYCLE_RECORD,
                {
                    "bk_cloud_id": src_cluster.bk_cloud_id,
                    "port": src_master_proxys[0].port,
                    "del_ips": src_master_proxy_ips,
                },
            ),
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.RECYCLE_RECORD,
                {
                    "bk_cloud_id": dst_cluster.bk_cloud_id,
                    "port": dst_master_proxys[0].port,
                    "del_ips": dst_master_proxy_ips,
                },
            ),
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.RECYCLE_RECORD,
                {
                    "bk_cloud_id": src_cluster.bk_cloud_id,
                    "port": src_is_standby_slave.port,
                    "del_ips": src_is_standby_slave.machine.ip,
                },
            ),
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.RECYCLE_RECORD,
                {
                    "bk_cloud_id": dst_cluster.bk_cloud_id,
                    "port": dst_is_standby_slave.port,
                    "del_ips": dst_is_standby_slave.machine.ip,
                },
            ),
        ]
        domain_add_list = [
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.CREATE,
                {
                    "bk_cloud_id": src_cluster.bk_cloud_id,
                    "port": src_master_proxys[0].port,
                    "add_ips": src_master_proxy_ips,
                    "entry": dst_master_entry.entry,
                },
            ),
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.CREATE,
                {
                    "bk_cloud_id": dst_cluster.bk_cloud_id,
                    "port": dst_master_entry[0].port,
                    "add_ips": dst_master_proxy_ips,
                    "entry": src_master_entry.entry,
                },
            ),
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.CREATE,
                {
                    "bk_cloud_id": src_cluster.bk_cloud_id,
                    "port": src_is_standby_slave.port,
                    "add_ips": src_is_standby_slave.machine.ip,
                    "entry": dst_is_standby_entry.entry,
                },
            ),
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.CREATE,
                {
                    "bk_cloud_id": dst_cluster.bk_cloud_id,
                    "port": dst_is_standby_slave.port,
                    "add_ips": dst_is_standby_slave.machine.ip,
                    "entry": src_is_standby_entry.entry,
                },
            ),
        ]

        exchange_sub_pipeline.add_parallel_acts(acts_list=domain_del_list)
        exchange_sub_pipeline.add_parallel_acts(acts_list=domain_add_list)
    elif src_cluster.cluster_type == ClusterType.TenDBSingle.value:
        src_master = src_cluster.storageinstance_set.get(instance_inner_role=InstanceInnerRole.ORPHAN.value)
        dst_master = dst_cluster.storageinstance_set.get(instance_inner_role=InstanceInnerRole.ORPHAN.value)
        src_master_entry = src_master.bind_entry.get(
            cluster_entry_type=ClusterEntryType.DNS.value, role=ClusterEntryRole.MASTER_ENTRY.value
        )
        dst_master_entry = dst_master.bind_entry.get(
            cluster_entry_type=ClusterEntryType.DNS.value, role=ClusterEntryRole.MASTER_ENTRY.value
        )
        domain_del_list = [
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.RECYCLE_RECORD,
                {"bk_cloud_id": src_cluster.bk_cloud_id, "port": src_master.port, "del_ips": src_master.machine.ip},
            ),
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.RECYCLE_RECORD,
                {"bk_cloud_id": dst_cluster.bk_cloud_id, "port": dst_master.port, "del_ips": dst_master.machine.ip},
            ),
        ]
        domain_add_list = [
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.CREATE,
                {
                    "bk_cloud_id": src_cluster.bk_cloud_id,
                    "port": src_master.port,
                    "add_ips": src_master.machine.ip,
                    "entry": dst_master_entry.entry,
                },
            ),
            BuildDNSManageSubflow(
                root_id,
                ticket_data,
                DnsOpType.CREATE,
                {
                    "bk_cloud_id": dst_cluster.bk_cloud_id,
                    "port": dst_master.port,
                    "add_ips": dst_master.machine.ip,
                    "entry": src_master_entry.entry,
                },
            ),
        ]
        exchange_sub_pipeline.add_parallel_acts(acts_list=domain_del_list)
        exchange_sub_pipeline.add_parallel_acts(acts_list=domain_add_list)
    else:
        raise Exception(_("域名互换不支持该集群类型"))
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

    #  todo 互换完毕修改元数据 db_meta_clusterentry
    return exchange_sub_pipeline.build_sub_process(sub_name=_("CLB域名互换"))
