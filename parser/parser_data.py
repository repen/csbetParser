from Model import CSGame
import json, re
from bs4 import BeautifulSoup
data_setting = {}

def pr(string):
    string = re.sub("\s{2,}", " ", string)
    return string

def printer(array):
    length = 0
    for arr in array:
        if len(arr) > length:
            length = len(arr)
    result = ""
    for arr in array:
        result += "{:^{}}\n".format(arr.strip(), length)
    result += "{:^{}}".format("=" * 55, length)
    return result

def hand_snapshot(snapshot):
    build = []
    main_str = ""
    html = snapshot
    soup = BeautifulSoup(html, "html.parser")
    result_page = soup.select_one(".content-inner ")
    if result_page:
        temp = ""
        team1_name = soup.select_one("div.btn-bet-head.sys-t1name.t1name").text.strip() if soup.select_one("div.btn-bet-head.sys-t1name.t1name") else "0"
        team2_name = soup.select_one("div.btn-bet-head.sys-t2name.t2name").text.strip() if soup.select_one("div.btn-bet-head.sys-t2name.t2name") else "0"
        team1_price = soup.select_one(".bm-fullbet-summ.sys-stat-abs-1.bet-currency.bet-currency_RUB").text.strip() if soup.select_one(".bm-fullbet-summ.sys-stat-abs-1.bet-currency.bet-currency_RUB") else "0"
        team2_price = soup.select_one(".bm-fullbet-summ.sys-stat-abs-2.bet-currency.bet-currency_RUB").text.strip() if soup.select_one(".bm-fullbet-summ.sys-stat-abs-2.bet-currency.bet-currency_RUB") else "0"

        team1_procent = soup.select_one(".sys-stat-proc-1").text.strip() if soup.select_one(".sys-stat-proc-1") else "0"
        team2_procent = soup.select_one(".sys-stat-proc-2").text.strip() if soup.select_one(".sys-stat-proc-2") else "0"

        team1_koef = soup.select_one(".stat-koef.sys-stat-koef-1").text.strip() if soup.select_one(".stat-koef.sys-stat-koef-1") else "0"
        team2_koef = soup.select_one(".stat-koef.sys-stat-koef-2").text.strip() if soup.select_one(".stat-koef.sys-stat-koef-2") else "0"

        name_series = soup.select_one(".bm-bo.sys-bo").text.strip() if soup.select_one(".bm-bo.sys-bo") else "0"
        name_liga = soup.select_one(".bm-champpic-text").text.strip() if soup.select_one(".bm-champpic-text") else "0"

        result = soup.select_one(".bm-result").text.strip() if soup.select_one(".bm-result") else "0"
        date_game = soup.select_one(".bm-date").text.strip() if soup.select_one(".bm-date") else "0"

        temp = "{0} ({2}р. {4} {6})     {8}    ({3}р. {5} {7}) {1}".format(
            team1_name, team2_name, team1_price, team2_price, team1_procent, team2_procent,
            team1_koef,team2_koef, result)
        build.extend([date_game, name_series, name_liga, temp])
        
        additional_event = soup.select(".bm-additional.bm-additional-common.a-betting-past")
        for additinal in additional_event:
            header = additinal.select_one(".bma-header-title").text.strip() if additinal.select_one(".bma-header-title") else "0"
            temp = "{}".format(header)
            build.append(temp)
            for bma in additinal.select(".bma-bet.sys-betting[class*=betting-won-team].betting-past"):
                team1_bma = bma.select_one(".btn-bet.sys-makebet-button").text.strip() if bma.select_one(".bm-team1") else "0"
                [x.extract() for x in bma.select(".bma-title-noresult, .bma-title-postponed")]
                name_bma  = bma.select_one(".bma-middle").text.strip() if bma.select_one(".bma-middle") else "0"
                team2_bma = bma.select_one(".bm-team2 .btn-bet.sys-makebet-button").text.strip() if bma.select_one(".bm-team2") else "0"
                temp = "{} \t| {} \t| {}".format(team1_bma, name_bma, team2_bma)
                build.append(temp)

        additional_event2 = soup.select(".bm-additional.a-betting-past:not(.bm-additional-common)")
        for additional in additional_event2:
            header = additional.select_one(".bma-header-title").text.strip() if additional.select_one(".bma-header-title") else "0"

            temp = "{}".format(header)
            build.append(temp)
            score = additional.select_one(".sys-betting[class*=betting-won-team].betting-past.has-score")
            team1score = score.select_one(".bm-team1").text.strip() if score.select_one(".bm-team1") else "0"
            name_score = score.select_one(".bma-middle").text.strip() if score.select_one(".bma-middle") else "0"
            team2score = score.select_one(".bm-team2").text.strip() if score.select_one(".bm-team2") else "0"
            temp += "{} \t| {} \t| {}".format(team1score, name_score, team2score)
            build.append(temp)
            score02 = additional.select_one(".bma-bet.bma-bet-map")
            team1score02 = score02.select_one(".btn-bet.btn-bet-sololine.sys-t1name.sys-makebet-button").text.strip() if score02.select_one(".btn-bet.btn-bet-sololine.sys-t1name.sys-makebet-button") else "0"
            name_score02 = score02.select_one(".bma-middle .bma-score").text.strip() if score02.select_one(".bma-middle .bma-score") else "0"
            team2score02 = score02.select_one(".btn-bet.btn-bet-sololine.sys-t2name.sys-makebet-button").text.strip() if score02.select_one(".btn-bet.btn-bet-sololine.sys-t2name.sys-makebet-button") else "0"

            temp = "{} \t| {} \t| {}".format(team1score02, name_score02, team2score02)
            build.append(temp)

            for item in soup.select(".bma-bet.sys-betting.betting-past"):
                [x.extract() for x in item.select(".bma-title-noresult, .bma-title-postponed")]
                bma_team1  = item.select_one(".bm-team1 .btn-bet.sys-makebet-button").text.strip() if item.select_one(".bm-team1 .btn-bet.sys-makebet-button") else "0"
                bma_middle = item.select_one(".bma-middle .bma-title").text.strip() if item.select_one(".bma-middle .bma-title") else "0"
                bma_team2  = item.select_one(".bm-team2 .btn-bet.sys-makebet-button").text.strip() if item.select_one(".bm-team2 .btn-bet.sys-makebet-button") else "0"

                temp = "{} \t| {} \t| {}\n".format(bma_team1, bma_middle, bma_team2)
                build.append(temp)

        main_str = printer(build)
    else:
        datetime  =  soup.select_one(".bet-match__additional-info-items").text.strip() if soup.select_one(".bet-match__additional-info-items") else "0"
        team1_name = soup.select_one(".bet-team__name.sys-t1name").text.strip() if soup.select_one(".bet-team__name.sys-t1name") else "0"
        team2_name = soup.select_one(".bet-team__name.sys-t2name").text.strip() if soup.select_one(".bet-team__name.sys-t2name") else "0"

        team1_price = soup.select_one(".bet-button__content-main.bet-currency.bet-currency_RUB.sys-stat-abs-1").text.strip() if soup.select_one(".bet-button__content-main.bet-currency.bet-currency_RUB.sys-stat-abs-1") else "0"
        team2_price = soup.select_one(".bet-button__content-main.bet-currency.bet-currency_RUB.sys-stat-abs-2").text.strip() if soup.select_one(".bet-button__content-main.bet-currency.bet-currency_RUB.sys-stat-abs-2") else "0"

        team1_koef = soup.select_one(".bet-button__content-more-item.bet-button__content-more-item_coeff.sys-stat-koef-1").text.strip() if soup.select_one(".bet-button__content-more-item.bet-button__content-more-item_coeff.sys-stat-koef-1") else "0"
        team1_procent = soup.select_one(".bet-button__content-more-item.bet-button__content-more-item_percent.sys-stat-proc-1").text.strip() if soup.select_one(".bet-button__content-more-item.bet-button__content-more-item_percent.sys-stat-proc-1") else "0"
        team2_koef = soup.select_one(".bet-button__content-more-item.bet-button__content-more-item_coeff.sys-stat-koef-2").text.strip() if soup.select_one(".bet-button__content-more-item.bet-button__content-more-item_coeff.sys-stat-koef-2") else "0"
        team2_procent = soup.select_one(".bet-button__content-more-item.bet-button__content-more-item_percent.sys-stat-proc-2").text.strip() if soup.select_one(".bet-button__content-more-item.bet-button__content-more-item_percent.sys-stat-proc-2") else "0"

        bet_items = soup.select(".bet-events__item")

        temp = "{0} ( {2}rub / {4} {6} ) VS ( {5} {7} / {3}rub ) {1}".format(
                team1_name, team2_name, team1_price, team2_price, team1_koef, team2_koef,
                team1_procent, team2_procent
            )
        build.extend([pr(datetime), temp])
        for item in bet_items:
            team1_item = item.select_one(".bet-currency.bet-currency_RUB.sys-stat-abs-1").text.strip()
            name_item  = item.select_one(".bet-event__text-inside-part").text.strip()
            team2_item = item.select_one(".bet-currency.bet-currency_RUB.sys-stat-abs-2").text.strip()
            build.append("{}\t| {}\t| {}".format(team1_item, name_item, team2_item))

        main_str = printer(build)

    return main_str
