from __future__ import annotations
from inspect import signature
from collections import defaultdict
 


class Graph:
    def __init__(self, vertices):
        self.graph = defaultdict(list)
        self.V = vertices
 
    def addEdge(self, u, v):
        self.graph[u].append(v)
 
    def topologicalSortUtil(self, v, visited, stack):
        visited[v] = True
        for i in self.graph[v]:
            if visited[i] == False:
                self.topologicalSortUtil(i, visited, stack)
        stack.append(v)
 
    def topologicalSort(self):
        visited = [False]*self.V
        stack = []
        for i in range(self.V):
            if visited[i] == False:
                self.topologicalSortUtil(i, visited, stack)
 
        return stack[::-1]



class Pipeline:
    def __init__(self, **metrics):
        self._metrics = []

        for name, metric in metrics.items():
            self._add_metric(name, metric)

        self._build_dep_tree()
        
    def _build_dep_tree(self):
        input_params = set({})
        graph = Graph(len(self._metrics))
        for i,m1 in enumerate(self._metrics):
            for param in m1[2].parameters.keys():
                found = False
                for j,m2 in enumerate(self._metrics):
                    if i == j:
                        continue
                    elif m2[0] == param:
                        graph.addEdge(j, i) # m2 has a dependant m1
                        found = True
                if not found:
                    input_params.add(param)
        execute_order = graph.topologicalSort()
        self._metrics = [self._metrics[idx] for idx in execute_order]
        self.input_params = input_params

    def _add_metric(self, name, metric):
        self._metrics.append((name, metric, signature(metric)))

    def _bind_args(self, sig, context):
        """ bind arguements from the context to the function sig"""
        args = []
        for param in sig.parameters.values():
            if param.default == param.empty:
                args.append(context[param.name])
            else:
                args.append(context.get(param.name, param.default))
        return args

    def __call__(self, context):
        response = {}
        for (name, metric, sig) in self._metrics:
            context[name] = metric(*self._bind_args(sig, context))
            response[name] = context[name]
        return response


if __name__ == "__main__":
    def sensor(id):
        return True


    def sensor_config(sensor):
        return sensor


    def sensor_risk(sensor, config, online, other):
        return sensor and config and online and other


    def sensor_online(config):
        return config


    def sensor_other(id, sensor, config):
        return id and sensor and config

    api_sensor_response = Pipeline(**{
        'sensor': sensor,
        'config': sensor_config,
        'risk': sensor_risk,
        'online': sensor_online,
        'other': sensor_other,
    })

    response = api_sensor_response({"id": "01010101"})
    print(response)
