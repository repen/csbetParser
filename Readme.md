### Описание проекта

1. [Selenium](https://github.com/repen/csbet_selenium)
2. [Парсер](https://github.com/repen/csbetParser)
3. [Сайт](https://github.com/repen/csbetSite)

### Команды

1. клонировать репо ```git clone https://github.com/repen/csbetParser.git```
2. Билд контейнера ```docker build -t csbet_parser .```
3. Запуск контейнера ```docker run --rm -d -v csbet:/usr/src/data --name csbetparser --network=mynet csbet_parser:latest```