"""续作批次同步子系统（岸端工单/船端离线任务/备缆仓库/出库单）。

设计要点：
- 批次整体落盘，按 batch_ref 幂等，写入失败后可凭完整批次恢复续作；
- 四类来源按"光缆+区段+里程并集"合并，属性或数量冲突留待人工裁决；
- 区段锁先到先得，后到船保留草稿；
- 库存/在途变化触发未出库需求重算，现场与岸端共用同一可用量视图。
"""
from .engine import SyncService
from .store import SyncStore

__all__ = ["SyncService", "SyncStore"]
