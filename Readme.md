### Описание проекта

Каккойто проект....

### Зависимости

**Docker**

### Команды

1. клонировать репо ```git clone```
2. Билд контейнера ```docker build -t csbet_parser .```
3. Запуск контейнера ```docker run --rm -d -v csbet:/usr/src/data --name csbetparser --network=mynet csbet_parser:latest```