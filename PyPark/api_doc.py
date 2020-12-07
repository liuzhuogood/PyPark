import yaml

from PyPark.API import PART_API
import logging
from multiprocessing import Process

from PyPark.result import Result
from PyPark.shootback.master import run_master
from PyPark.util.net import get_random_port

"""
{
                home:{
                    name:'说明',
                    desc:'',
                },
                apis: [{
                    tab: '请求',
                    url: "/add", method: "POST", name: "新增", desc: "这是一个新增接口",
                    request: {
                        desc: "请求参数",
                        type: "json",
                        params: [
                            {name: 'Frozen Yogurt', default: 159, type: 'str', required: true, desc: 'aaaaaaaaaaaa'},
                            {name: 'Frozen Yogurt21', default: 159, type: 'str', required: true, desc: 'aaaaaaaaaaaa'},
                            {name: 'Frozen Yogurt3', default: 159, type: 'str', required: true, desc: 'aaaaaaaaaaaa'},
                            {name: 'Frozen Yogurt4', default: 159, type: 'str', required: true, desc: 'aaaaaaaaaaaa'},
                            {name: 'Frozen Yogurt5', default: 159, type: 'str', required: true, desc: 'aaaaaaaaaaaa'},
                        ]
                    },
                    response: {
                        desc: "响应参数",
                        type: "json",
                        params: [
                            {name: 'Frozen Yogurt', default: 159, type: 'str', required: true, desc: 'aaaaaaaaaaaa'},
                            {name: 'Frozen Yogurt21', default: 159, type: 'str', required: true, desc: 'aaaaaaaaaaaa'},
                            {name: 'Frozen Yogurt3', default: 159, type: 'str', required: true, desc: 'aaaaaaaaaaaa'},
                            {name: 'Frozen Yogurt4', default: 159, type: 'str', required: true, desc: 'aaaaaaaaaaaa'},
                            {name: 'Frozen Yogurt5', default: 159, type: 'str', required: true, desc: 'aaaaaaaaaaaa'},
                        ]
                    }
                },
                    {
                        tab: '请求',
                        url: "/delete", method: "POST", name: "删除", desc: "这是一个删除接口",
                        request: {
                            desc: "请求参数",
                            type: "text",
                            params: "dddddddddddddddddd"
                        },
                        response: {
                            desc: "响应参数",
                            type: "json",
                            params: [
                                {name: "name", default: "name", type: "str", required: true, desc: "名称"}
                            ]
                        }
                    },

                ],
                tabs: [],
                leftDrawerOpen: false,
                columns: [
                    {name: 'name', required: true, label: '参数名', sortable: true, field: 'name', align: "left"},
                    {name: 'type', label: '类型', field: 'type', align: "center"},
                    {name: 'default', label: '默认值', field: 'default', align: "left"},
                    {name: 'required', label: '要求', field: 'required', align: "center"},
                    {name: 'desc', label: '说明', field: 'desc', align: "left"}
                ]


            }
        }
"""


class ApiDoc(object):

    def __init__(self, app):
        self.app = app

        @self.app.service(path=PART_API.PYPARK_APIS)
        def pypark_apis(data):
            return self.__apiDoc(data)

    def __apiDoc(self, data):
        pass
