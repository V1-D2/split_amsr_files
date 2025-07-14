#!/bin/bash
#SBATCH --job-name=split_amsr_files
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
#SBATCH --partition=salvador
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=02:00:00

echo "============================================"
echo "AMSR-2 File Splitter Job Started: $(date)"
echo "Node: $SLURMD_NODENAME"
echo "============================================"

# Set environment variables
export APPTAINER_QUIET=1
export SINGULARITY_QUIET=1

# Переходим в директорию проекта
cd $HOME/split_amsr_files

# Проверяем среду
echo "Testing environment:"
apptainer exec --nv \
    --bind $HOME/local-python:$HOME/.local \
    /home/shared/containers/tensorflow-25.02-py3.sif \
    python -c "
import sys
print(f'Python: {sys.version}')
try:
    import numpy as np
    print(f'✅ NumPy: {np.__version__}')
except Exception as e:
    print(f'❌ NumPy: {e}')
try:
    from tqdm import tqdm
    print(f'✅ tqdm: installed')
except Exception as e:
    print(f'❌ tqdm: {e}')
"

# Устанавливаем необходимые пакеты если их нет
echo "Installing required packages..."
apptainer exec --nv \
    --bind $HOME/local-python:$HOME/.local \
    /home/shared/containers/tensorflow-25.02-py3.sif \
    pip install --user numpy tqdm

echo "============================================"
echo "Starting file splitting process:"

# Запускаем процесс разделения файлов
apptainer exec --nv \
    --bind $HOME/local-python:$HOME/.local \
    --bind $HOME/split_amsr_files:$HOME/split_amsr_files \
    --bind /home/vdidur/temperature_sr_project/data:/home/vdidur/temperature_sr_project/data \
    /home/shared/containers/tensorflow-25.02-py3.sif \
    python split_amsr_files.py

echo "============================================"
echo "AMSR-2 File Splitter Job Finished: $(date)"
echo "============================================"

# Показываем результаты
echo "Checking results:"
ls -la /home/vdidur/temperature_sr_project/data/*_part_*of*.npz | head -20
echo "..."
echo "Total split files created:"
ls /home/vdidur/temperature_sr_project/data/*_part_*of*.npz | wc -l