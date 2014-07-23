import re

import pandas as pd
import numpy as np
from collections import defaultdict
from functools import reduce

boolOp = re.compile("( or )|( and )|(&)|(\|)")
arithOp = re.compile("(==)|(=)|(!=)|(>=)|(>)|(<=)|(<)")

class Store(pd.HDFStore):
    def add(self, key, df, indexes=None):
        self.indexes = indexes
        if indexes:
            for index in indexes:
                if index not in df:
                    raise KeyError("Column not found: {}".format(index))
                    
            folder = key + "/"
            indLookup = dict(enumerate(df.index))
            self[folder + "full"] = df
            self[folder + "index"] = pd.Series(indLookup)
            
            largest = len(df)
            
            lookup = defaultdict(list)
            
            for ind in range(largest):
                index = indLookup[ind]
                path = folder + self.getPath(ind)
                lookup[path].append(index)
            
            for key, value in lookup.items():
                self[key] = df.loc[value]
            
            for index in indexes:
                self[folder + str(index)] = pd.Series(dict(enumerate(df[index])))
        else:
            self[key] = df

    def _eval(self, key, where):
        match = arithOp.search(where)
        column = where[:match.start()].strip()
        other = where[match.start():]
        c = self[key + "/" + column]
        return c[pd.eval("c" + other)].index 
    
    def select(self, key, where, start=None, stop=None, columns=None,
               iterator=False, chunksize=None, auto_close=False):
        if key + "/full" in self:
            match = boolOp.search(where)
            if match and match.start() < where.find("("):
                this = where[match.start()].strip()
            elif "(" in where:
                pass
            else:   # only single expression
                index = self._eval(key, where)
                
            return reduce(pd.merge, (self[key + "/" + self.getPath(i)]
                                    for i in index))
                
            
        else:
            return pd.HDFStore.select(self, key, where, start, stop, columns,
                                      iterator, chunksize, auto_close)

    
    def select_column(self, key, column, **kwargs):
        key += "/"
        if key + column in self:
            s = self[keys + column]
            s.index = self[keys + "index"]
            return s
        else:
            return pd.HDFStore.select_column(key, column, **kwargs)
    def getPath(self, key):
        return "data/" + hex(key)[1:]

s = Store("test.hd5")

t = {"test": {"a":"b", "c": "werwer", "d" : "poiupo"},
     "good": {"a":1, "b":2, "c":3, "d": 4}     
     }
df = pd.DataFrame.from_dict(t)
s.add("df", df, indexes=["test"])
