#!/usr/bin/env python3
"""
AMSR-2 File Splitter - Разделяет большие NPZ файлы на маленькие части
"""

import pathlib
import numpy as np
import os
from tqdm import tqdm

# ПУТЬ К ПАПКЕ С ФАЙЛАМИ - ИЗМЕНИТЕ ПРИ НЕОБХОДИМОСТИ
DATA_DIR = pathlib.Path("/data/users/vdidur/data")
NEW_DATA_DIR = DATA_DIR / "/data/users/vdidur/data/new_data_all/"

# Размер каждой части (количество точек)
CHUNK_SIZE = 500


def split_npz_file(file_path: pathlib.Path, chunk_size: int = CHUNK_SIZE):
    """Разделяет один NPZ файл на несколько маленьких"""

    print(f"\nОбработка файла: {file_path.name}")

    try:
        # Загружаем данные
        with np.load(file_path, allow_pickle=True) as data:
            swath_array = data['swath_array']
            period = data.get('period', 'Unknown period')

        # Преобразуем в список если нужно
        if isinstance(swath_array, np.ndarray):
            swath_list = swath_array.tolist()
        else:
            swath_list = list(swath_array)

        total_swaths = len(swath_list)
        print(f"Найдено точек данных: {total_swaths}")

        # Вычисляем количество частей
        num_chunks = (total_swaths + chunk_size - 1) // chunk_size
        print(f"Будет создано файлов: {num_chunks}")

        # Создаем базовое имя для новых файлов
        base_name = file_path.stem  # имя без расширения

        # Разделяем и сохраняем
        for i in tqdm(range(num_chunks), desc="Создание файлов"):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_swaths)

            # Берем часть данных
            chunk_data = swath_list[start_idx:end_idx]

            # Создаем имя нового файла
            chunk_name = f"{base_name}_part_{i + 1}of{num_chunks}.npz"
            NEW_DATA_DIR.mkdir(exist_ok=True)
            chunk_path = NEW_DATA_DIR / chunk_name

            # Сохраняем
            save_dict = {
                'swath_array': chunk_data,
                'period': period,
                'chunk_info': {
                    'original_file': file_path.name,
                    'part_number': i + 1,
                    'total_parts': num_chunks,
                    'chunk_size': len(chunk_data),
                    'start_index': start_idx,
                    'end_index': end_idx
                }
            }

            np.savez_compressed(chunk_path, **save_dict)

            # Проверяем размер созданного файла
            file_size_mb = chunk_path.stat().st_size / (1024 * 1024)

        print(f"✓ Успешно создано {num_chunks} файлов для {file_path.name}")

        # Опционально: спросить об удалении оригинала
        # if input(f"\nУдалить оригинальный файл {file_path.name}? (y/n): ").lower() == 'y':
        #     file_path.unlink()
        #     print("Оригинальный файл удален")

    except Exception as e:
        print(f"✗ Ошибка при обработке {file_path.name}: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Главная функция"""

    print("=== AMSR-2 File Splitter ===")
    print(f"Папка с данными: {DATA_DIR}")
    print(f"Размер частей: {CHUNK_SIZE} точек\n")

    # Проверяем существование папки
    if not DATA_DIR.exists():
        print(f"ОШИБКА: Папка {DATA_DIR} не существует!")
        return

    # Находим все NPZ файлы
    npz_files = list(DATA_DIR.glob("*.npz"))

    if not npz_files:
        print("NPZ файлы не найдены в указанной папке!")
        print("Убедитесь, что файлы имеют формат: AMSR2_temp_only_20200101_000000_to_20210101_000000.npz")
        return

    print(f"Найдено файлов для разделения: {len(npz_files)}")
    for f in npz_files:
        file_size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  - {f.name} ({file_size_mb:.1f} MB)")

    # Обрабатываем каждый файл
    for npz_file in npz_files:
        split_npz_file(npz_file, CHUNK_SIZE)

    print("\n=== Разделение завершено ===")

    # Показываем статистику
    all_parts = list(DATA_DIR.glob("*_part_*of*.npz"))
    if all_parts:
        print(f"Всего создано частей: {len(all_parts)}")
        total_size = sum(f.stat().st_size for f in all_parts) / (1024 * 1024)
        print(f"Общий размер всех частей: {total_size:.1f} MB")


if __name__ == "__main__":
    main()