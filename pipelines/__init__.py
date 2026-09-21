import os
import sys
from datetime import datetime


# Ruta absoluta del directorio actual (config/)
current_dir = os.path.dirname(os.path.abspath(__file__))

# Subir un nivel (root del proyecto: numenor_source/)
parent_dir = os.path.dirname(current_dir)

# numenor_source/libs/ → para que data_engineering_toolbox se importe directamente
libs_dir = os.path.join(parent_dir, "libs")

print(f"Current directory:  {current_dir}")
print(f"Parent directory:  {parent_dir}")
print(f"Libs directory:    {libs_dir}")

for path in [parent_dir, libs_dir]:
    if path not in sys.path:
        sys.path.insert(0, path)
