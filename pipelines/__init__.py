import os
import sys


# Asegura que el root del proyecto esté en sys.path (imports tipo `libs.*`, `config.*`)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
