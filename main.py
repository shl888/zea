#!/usr/bin/env python3
"""
Zeabur强制入口文件 - 仅作为委托，不破坏原有代码结构
"""

import sys
import os

# 将当前目录加入Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# 直接委托给brain_core.py
if __name__ == "__main__":
    from brain_core import main
    main()
