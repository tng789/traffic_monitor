from collections import deque

class FixedFIFO:
    """固定长度的FIFO列表，新元素挤掉最老元素"""
    
    def __init__(self, maxlen):
        if maxlen <= 0:
            raise ValueError("maxlen 必须大于 0")
        self._data = deque(maxlen=maxlen)
    
    def push(self, item):
        """添加新元素，自动挤掉最老的元素"""
        self._data.append(item)
    
    def get(self, index):
        """通过索引访问元素，支持负索引"""
        return self._data[index]
    
    def get_all(self):
        """获取所有元素（从最老到最新）"""
        return list(self._data)
    
    def get_oldest(self):
        """获取最老的元素"""
        if not self._data:
            raise IndexError("FIFO 为空")
        return self._data[0]
    
    def get_newest(self):
        """获取最新的元素"""
        if not self._data:
            raise IndexError("FIFO 为空")
        return self._data[-1]
    
    def is_full(self):
        """判断是否已满"""
        return len(self._data) == self._data.maxlen
    
    def __len__(self):
        return len(self._data)
    
    def __iter__(self):
        return iter(self._data)
    
    def __repr__(self):
        return f"FixedFIFO(maxlen={self._data.maxlen}, data={list(self._data)})"


# ============ 使用示例 ============
#if __name__ == "__main__":
#    fifo = FixedFIFO(5)
#    
#    # 添加元素
#    for i in range(7):
#        fifo.push(i)
#        print(f"加入 {i} 后: {fifo}")
#    
#    print("\n--- 索引访问演示 ---")
#    print(f"最老的元素 [0]: {fifo.get(0)}")
#    print(f"最新的元素 [-1]: {fifo.get(-1)}")
#    print(f"中间元素 [2]: {fifo.get(2)}")
#    print(f"所有元素: {fifo.get_all()}")
#    print(f"当前长度: {len(fifo)}")
#    print(f"是否已满: {fifo.is_full()}")
#    
#    print("\n--- 遍历演示 ---")
#    for item in fifo:
#        print(f"元素: {item}")