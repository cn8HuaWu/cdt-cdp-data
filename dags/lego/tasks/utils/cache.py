from collections import namedtuple, defaultdict
from typing import TypeVar
import abc

T = TypeVar('T')
class Cache(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        self._cache_dict = defaultdict(lambda: None)

    @abc.abstractmethod
    def add(self, T):
        pass

    @abc.abstractmethod    
    def search(self, keyname) -> T:
        pass

ModifiedProduct = namedtuple("ModifiedProduct", "distributor lego_sku_id should_be_sku_id action_flag")
class ModifiedProductCache(Cache):
    def __init__(self) -> None:
        super().__init__()
        self._initialized = False

    def is_initialized(self):
        return self._initialized

    def init_from_list(self, product_list):
        for prd in product_list:
           self.add(prd)
        self._initialized = True

    def get_size(self):
        return len(self._cache_dict)

    def gen_key(self, product:ModifiedProduct) -> str:
        if product.distributor is None or product.lego_sku_id is None:
            raise ValueError("ModifiedProductCache.add(): distributor and lego_sku_id must not empty")
        return product.lego_sku_id

    def add(self, product:ModifiedProduct):
        self._cache_dict[self.gen_key(product)] =  product
        self._initialized = True
    
    def search(self, keyname) -> ModifiedProduct:
        return self._cache_dict[keyname]


from sys import getsizeof
if __name__ == "__main__":
    pass
    # productcache = ModifiedProductCache()    
    # cache_query = "select * from edw.d_dl_modified_product"
    # productcache.init_from_db(db, cache_query)
    # print( productcache.search("LCS_92943") is None )


