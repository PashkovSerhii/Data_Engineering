FROM python:3.10-slim

# Встановлюємо змінні середовища для оптимізації
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Встановлюємо робочу директорію
WORKDIR /app

# Встановлюємо системні залежності (за потреби)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Копіюємо залежності
COPY requirements.txt .

# Встановлюємо Python-залежності
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копіюємо код програми
COPY transforms_data.py .

# Команда за замовчуванням
CMD ["python", "transforms_data.py"]