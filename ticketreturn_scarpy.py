# -*- coding: utf-8 -*-
import scrapy
from scrapy.crawler import CrawlerProcess
import os, datetime
import middlewares
from bs4 import BeautifulSoup
import re
import csv

class ticietreturn(scrapy.Spider):
    name = 'ticietreturn'
    allowed_domains = ['']
    start_urls = []

    months = {
        'JAN': '1',
        'FEB': '2',
        'MAR': '3',
        'APR': '4',
        'MAY': '5',
        'JUN': '6',
        'JUL': '7',
        'AUG': '8',
        'SEP': '9',
        'OCT': '10',
        'NOV': '11',
        'DEC': '12',
    }

    fieldnames = [
        'EventName',
        'EventDate',
        'EventTime',
        'Quantity',
        'Section',
        'Row',
        'LowSeat',
        'WholesalePrice',
        'OnHandDate',
        'Notes',
        'TicketGroupTypeID',
        'StockTypeID',
        'ShippingMethodID',
        'ShowNearTermOptionID',
    ]

    def ranges(self, nums):
        nums = sorted(set(nums))
        gaps = [[s, e] for s, e in zip(nums, nums[1:]) if s + 1 < e]
        edges = iter(nums[:1] + sum(gaps, []) + nums[-1:])
        return list(zip(edges, edges))

    def convert_12_to_24(self, time):
        time = time.upper()

        if 'AM' not in time or 'PM' not in time:
            return time

        am_pm = time.split()[1]

        if am_pm.upper() == 'AM':
            return time.split()[0]

        hours = time.split()[0].split(":")[0].strip()
        minutes = time.split()[0].split(":")[1].strip()

        return str(int(hours) + 12) + ":" + minutes

    def get_event_id(self, url):
        event_id_regex = re.match(r".+EventID=(\d+)&continue.+", url)

        try:
            return event_id_regex.group(1)

        except Exception:
            return None

    def get_sections_and_price_ratings(self, html):
        all_sections = [k.replace('"', '') for k in
                        html.split('var SecS2 = new Array(').pop().split(');')[0].split(",")]

        split = html.split("SelectSeatsLink")

        price_ratings = {}

        price_ratings_list = []
        price_list = []
        sections_list = []

        for ctr, each in enumerate(split):
            price_rating_regex = re.findall(r".*NAME='PriceRating' VALUE='(.*?)'.*", each)

            try:
                price_ratings_list.append(price_rating_regex[0])

            except Exception as e:
                pass

            price_regex = re.findall(r".*NAME='Price' VALUE='\$(.*?)'.*", each)

            try:
                price_list.append(price_regex[0])

            except Exception as e:
                pass

            sections_regex = re.findall(r".*<option value='.*\^.*>Section (.*?)</option>.*", each)
            try:
                sections_list.append(sections_regex)
            except Exception as e:
                print(str(e))

        for ctr, each_section in enumerate(sections_list):
            for single in each_section:
                if single in all_sections:
                    price_ratings[single] = {
                        'price_rating': price_ratings_list[ctr],
                        'price': price_list[ctr]
                    }

        return price_ratings

    def get_section_levels(self, html):
        all_sections = [k.replace('"', '') for k in
                        html.split('var SecS2 = new Array(').pop().split(');')[0].split(",")]
        all_levels = [k.replace('"', '') for k in html.split('var SecS1 = new Array(').pop().split(');')[0].split(",")]
        all_section_levels = {}
        for ctr, section in enumerate(all_sections):
            all_section_levels[section] = all_levels[ctr]

        return all_section_levels


    def start_requests(self):
        yield scrapy.FormRequest('https://www.ticketreturn.com/boxoffice/Categories.aspx',method='GET',
                                headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36"})


    def parse(self, response):
        print(response)
        All_Url_link = response.xpath("//a[contains(@href,'/team.asp?sponsorid')]/@href").extract()
        for link in All_Url_link:
            link = 'https://www.ticketreturn.com'+link
            yield scrapy.FormRequest(link,method='GET',callback=self.sponsorid,dont_filter=True,
                                     headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36"})

    def sponsorid(self, response):
        print(response)

        strhtml = response.body
        strhtml = strhtml.decode("ascii","ignore")

        soup = BeautifulSoup(strhtml, features='lxml')

        for tr in soup.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) != 3 or tds[2].text != 'Buy Tickets':
                continue

            date_time_split = tds[1].text.replace("at ", " at ").split(" at ")

            date_split = date_time_split[0].split(" ")

            if not date_split or len(date_split) != 4:
                continue

            date = self.months[date_split[1].strip().upper()] + "/" + date_split[2] + "/" + date_split[3]

            event_url = 'https://www.ticketreturn.com/prod2/' + tds[2].find('a').get('href') + "&continue=buynew.asp"

            event_data = {
                'date': date,
                'time': self.convert_12_to_24(date_time_split[1].strip()),
                'event_url': event_url,
                'event_name': tds[0].text.replace(" at", " at ")
            }

            yield scrapy.FormRequest(event_url, method='GET', callback=self.eventdata, dont_filter=True,
                                     headers={
                                         "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36"},
                                     meta={'event_data':event_data})

    def eventdata(self, response):
        print(response)

        event_data = response.meta['event_data']
        strhtml = response.body
        strhtml = strhtml.decode("ascii", "ignore")
        event_id = self.get_event_id(event_data['event_url'])

        sections_and_price_rating = self.get_sections_and_price_ratings(strhtml)

        sections_and_levels = self.get_section_levels(strhtml)

        for section in sections_and_price_rating:
            query_url = f"https://www.ticketreturn.com/prod2/buysectionNew.asp?NoEdit=no&EventID={event_id}&PriceRating={sections_and_price_rating[section]['price_rating']}&Level={sections_and_levels[section]}&Section={section}&NumTickets=0&NumTickets2=0"
            inv = {'sections_and_levels': sections_and_levels,
                   'event_id': event_id,
                   'sections_and_price_rating': sections_and_price_rating,
                   'event_data': event_data,
                   'section': section}
            print(query_url)
            yield scrapy.FormRequest(query_url, method='GET', callback=self.finaldata, dont_filter=True,
                                     headers={
                                         "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36"},
                                     meta={'inv': inv})



    def finaldata(self,response):
        print(response)

        inv = response.meta['inv']
        sections_and_price_rating = inv['sections_and_price_rating']
        event_data = inv['event_data']
        section = inv['section']
        event_id = inv['event_id']

        inv_html = response.body
        inv_html = inv_html.decode("ascii", "ignore")

        section_row_info = {}

        inv_data = []

        seats_regex = re.findall(r".*title='(.*?)'></A></SPAN>.*", inv_html)

        type_regex = re.findall(r".*SRC='TRImages/(.*?).gif' border=.*", inv_html)

        for ctr, entry in enumerate(seats_regex):
            row = re.findall(r".*Row-(.*?)Seat-.*", entry)[0].strip()
            seat = re.findall(r".*Row-.*?Seat-(.*)", entry)[0]

            if section not in section_row_info:
                section_row_info[section] = {
                    'price': sections_and_price_rating[section]['price'],
                    'rows': {}
                }

            if row not in section_row_info[section]['rows']:
                section_row_info[section]['rows'][row] = {
                    'regular': [],
                    'handicapped': [],
                    'wheelchair': []
                }

            if 'hand' in type_regex[ctr]:
                section_row_info[section]['rows'][row]['handicapped'].append(seat)
                continue

            if seat.startswith('*') and seat.endswith('*'):
                section_row_info[section]['rows'][row]['wheelchair'].append(seat.replace('*', ''))
                continue

            if seat.isnumeric():
                section_row_info[section]['rows'][row]['regular'].append(int(seat))

        for each_section in section_row_info:
            for each_row in section_row_info[each_section]['rows']:
                for each_handicapped in section_row_info[each_section]['rows'][each_row]['handicapped']:
                    inv_data.append({
                        'Quantity': '1',
                        'Section': each_section,
                        'Row': each_row,
                        'LowSeat': each_handicapped,
                        'WholesalePrice': section_row_info[each_section]['price'],
                        'OnHandDate': '',
                        'Notes': 'Handicapped',
                        'TicketGroupTypeID': '',
                        'StockTypeID': '',
                        'ShippingMethodID': '',
                        'ShowNearTermOptionID': '',
                        'EventName': event_data['event_name'],
                        'EventDate': event_data['date'],
                        'EventTime': event_data['time'],
                    })
                for each_wheelchair in section_row_info[each_section]['rows'][each_row]['wheelchair']:
                    inv_data.append({
                        'Quantity': '1',
                        'Section': each_section,
                        'Row': each_row,
                        'LowSeat': each_wheelchair,
                        'WholesalePrice': section_row_info[each_section]['price'],
                        'OnHandDate': '',
                        'Notes': 'Wheelchair',
                        'TicketGroupTypeID': '',
                        'StockTypeID': '',
                        'ShippingMethodID': '',
                        'ShowNearTermOptionID': '',
                        'EventName': event_data['event_name'],
                        'EventDate': event_data['date'],
                        'EventTime': event_data['time'],
                    })

                grouped = self.ranges(section_row_info[each_section]['rows'][each_row]['regular'])

                for group in grouped:
                    inv_data.append({
                        'Quantity': group[1] - group[0] + 1,
                        'Section': each_section,
                        'Row': each_row,
                        'LowSeat': group[0],
                        'WholesalePrice': section_row_info[each_section]['price'],
                        'OnHandDate': '',
                        'Notes': '',
                        'TicketGroupTypeID': '',
                        'StockTypeID': '',
                        'ShippingMethodID': '',
                        'ShowNearTermOptionID': '',
                        'EventName': event_data['event_name'],
                        'EventDate': event_data['date'],
                        'EventTime': event_data['time'],
                    })

        filename = str(event_id) + '-data.csv'

        file_exists = os.path.isfile(filename)
        if file_exists:
            f = open(str(event_id) + '-data.csv', 'a')
        else:
            f = open(str(event_id) + '-data.csv', 'w')

        with f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames, delimiter=',', lineterminator='\n')
            if not file_exists:
                writer.writeheader()
            if inv_data != []:
                writer.writerows(rowdicts=inv_data)


    def close(spider, reason):
        print("Successfully Done...........................")




process = CrawlerProcess({
                          'CONCURRENT_REQUESTS':200,
                          # 'DOWNLOADER_MIDDLEWARES': {
                          #       'middlewares.CustomProxyMiddleware': 500,
                          #   },
                        })
process.crawl(ticietreturn)
try:
    process.start()
except:
    pass





