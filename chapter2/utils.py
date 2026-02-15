from collections import namedtuple
import datetime
import time
from loguru import logger
import json
import requests
import copy
import yaml

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode
import pandas as pd


class KoreaInvestEnv:
    def __init__(self, cfg):
        self.cfg = cfg
        self.custtype = cfg['custtype']
        self.base_headers = {
            "Content-Type": "application/json",
            "Accept": "text/plain",
            "charset": "UTF-8",
            'User-Agent': cfg['my_agent'],
        }
        is_paper_trading = cfg['is_paper_trading']
        if is_paper_trading:
            using_url = cfg['paper_url']
            api_key = cfg['paper_api_key']
            api_secret_key = cfg['paper_api_secret_key']
            account_num = cfg['paper_stock_account_number']
            future_account_num = cfg['paper_future_account_number']
        else:
            using_url = cfg['url']
            api_key = cfg['api_key']
            api_secret_key = cfg['api_secret_key']
            account_num = cfg['stock_account_number']
            future_account_num = cfg['future_account_number']
        websocket_approval_key = self.get_websocket_approval_key(using_url, api_key, api_secret_key)
        account_access_token = self.get_account_access_token(using_url, api_key, api_secret_key)
        self.base_headers["authorization"] = account_access_token
        self.base_headers["appkey"] = api_key
        self.base_headers["appsecret"] = api_secret_key
        self.cfg['websocket_approval_key'] = websocket_approval_key
        self.cfg['account_num'] = account_num
        self.cfg['future_account_num'] = future_account_num
        self.cfg['using_url'] = using_url
    
    def get_base_headers(self):
        return copy.deepcopy(self.base_headers)

    def get_full_config(self):
        return copy.deepcopy(self.cfg)

    def get_account_access_token(self, request_base_url='', api_key='', api_secret_key=''):
        # 계좌에 접근 가능한 토큰 발급
        p = {
            "grant_type": "client_credentials",
            "appkey": api_key,
            "appsecret": api_secret_key,
        }

        url = f'{request_base_url}/oauth2/tokenP'

        res = requests.post(url, data=json.dumps(p), headers=self.base_headers)
        res.raise_for_status()
        my_token = res.json()['access_token']
        return f"Bearer {my_token}"

    def get_websocket_approval_key(self, request_base_url='', api_key='', api_secret_key=''):
        # 웹소켓 접속키 발급
        headers = {"content-type": "application/json"}
        body = {
            "grant_type": "client_credentials",
            "appkey": api_key,
            "secretkey": api_secret_key,
        }
        URL = f"{request_base_url}/oauth2/Approval"
        res = requests.post(URL, headers=headers, data=json.dumps(body))
        approval_key = res.json()["approval_key"]
        return approval_key


class KoreaInvestAPI:
    def __init__(self, cfg, base_headers):
        self.custtype = cfg['custtype']
        self._base_headers = base_headers
        self.websocket_approval_key = cfg['websocket_approval_key']
        self.account_num = cfg['account_num']
        self.future_account_num = cfg['future_account_num']
        self.is_paper_trading = cfg['is_paper_trading']
        self.htsid = cfg['htsid']
        self.using_url = cfg['using_url']

    def set_order_hash_key(self, h, p):
        # 주문 API에서 사용할 hash key값을 받아 header에 설정해 주는 함수
        # Input: HTTP Header, HTTP post param
        # Output: None
        url = f"{self.using_url}/uapi/hashkey"

        res = requests.post(url, data=json.dumps(p), headers=h)
        rescode = res.status_code
        if rescode == 200:
            h['hashkey'] = res.json()['HASH']
        else:
            logger.info(f"Error: {rescode}")

    def _url_fetch(self, api_url, tr_id, params, is_post_request=False, use_hash=True):
        try:
            url = f"{self.using_url}{api_url}"
            headers = self._base_headers

            # 추가 Header 설정
            tr_id = tr_id
            if tr_id[0] in ('T', 'J', 'C'):
                if self.is_paper_trading:
                    tr_id = 'V' + tr_id[1:]

            headers["tr_id"] = tr_id
            headers["custtype"] = self.custtype

            if is_post_request:
                if use_hash:
                    self.set_order_hash_key(headers, params)
                res = requests.post(url, headers=headers, data=json.dumps(params))
            else:
                res = requests.get(url, headers=headers, params=params)

            if res.status_code == 200:
                ar = APIResponse(res)
                return ar
            else:
                logger.info(f"Error Code : {res.status_code} | {res.text}")
                return None
        except Exception as e:
            logger.info(f"URL exception: {e}")

    def get_overseas_acct_balance(self):
        # 계좌 잔고를 평가잔고와 상세 내역을 DataFrame 으로 반환
        url = '/uapi/overseas-stock/v1/trading/inquire-balance'
        if self.is_paper_trading:
            tr_id = "VTTS3012R"
        else:
            tr_id = "TTTS3012R"
        params = {
            'CANO': self.account_num,
            'ACNT_PRDT_CD': '01',
            'OVRS_EXCG_CD': 'NASD',
            'TR_CRCY_CD': 'USD',
            'CTX_AREA_FK200': '',
            'CTX_AREA_NK200': '',
        }

        t1 = self._url_fetch(url, tr_id, params)
        output_columns = ['종목코드', '해외거래소코드', '종목명', '보유수량', '매도가능수량', '매입단가', '수익률', '현재가', '평가손익']
        if t1 is None:
            return 0, pd.DataFrame(columns=output_columns)

        try:
            output1 = t1.get_body().output1
        except Exception as e:
            logger.info(f"Exception: {e}, t1: {t1}")
            return 0, pd.DataFrame(columns=output_columns)
        if t1 is not None and t1.is_ok() and output1:  # body 의 rt_cd 가 0 인 경우만 성공
            df = pd.DataFrame(output1)
            target_columns = ['ovrs_pdno', 'ovrs_excg_cd', 'ovrs_item_name', 'ovrs_cblc_qty', 'ord_psbl_qty', 'pchs_avg_pric', 'evlu_pfls_rt', 'now_pric2', 'frcr_evlu_pfls_amt']
            df = df[target_columns]
            df[target_columns[3:]] = df[target_columns[3:]].apply(pd.to_numeric)
            column_name_map = dict(zip(target_columns, output_columns))
            df.rename(columns=column_name_map, inplace=True)
            df = df[df['보유수량'] != 0]
            r2 = t1.get_body().output2
            return float(r2['tot_evlu_pfls_amt']), df
        else:
            return 0, pd.DataFrame(columns=['종목코드', '해외거래소코드', '종목명', '보유수량', '매도가능수량', '매입단가', '수익률', '현재가', '평가손익'])

    def get_acct_balance(self):
        # 계좌 잔고 평가 잔고와 상세 내역을 DataFrame 으로 반환
        url = '/uapi/domestic-stock/v1/trading/inquire-balance'
        if self.is_paper_trading:
            tr_id = "VTTC8434R"
        else:
            tr_id = "TTTC8434R"

        params = {
            'CANO': self.account_num,
            'ACNT_PRDT_CD': '01',
            'AFHR_FLPR_YN': 'N',
            'FNCG_AMT_AUTO_RDPT_YN': 'N',
            'FUND_STTL_ICLD_YN': 'N',
            'INQR_DVSN': '01',
            'OFL_YN': 'N',
            'PRCS_DVSN': '01',
            'UNPR_DVSN': '01',
            'CTX_AREA_FK100': '',
            'CTX_AREA_NK100': ''
        }

        t1 = self._url_fetch(url, tr_id, params)
        output_columns = ['종목코드', '종목명', '보유수량', '매도가능수량', '매입단가', '수익률', '현재가', '전일대비', '전일대비 등락률']
        if t1 is None:
            return 0, pd.DataFrame(columns=output_columns)
        try:
            output1 = t1.get_body().output1
        except Exception as e:
            logger.info(f"Exception: {e}, t1: {t1}")
            return 0, pd.DataFrame(columns=output_columns)
        if t1 is not None and t1.is_ok() and output1:  # body 의 rt_cd 가 0 인 경우만 성공
            df = pd.DataFrame(output1)
            target_columns = [
                'pdno',
                'prdt_name',
                'hldg_qty',
                'ord_psbl_qty',
                'pchs_avg_pric',
                'evlu_pfls_rt',
                'prpr',
                'bfdy_cprs_icdc',
                'fltt_rt',
            ]
            df = df[target_columns]
            df[target_columns[2:]] = df[target_columns[2:]].apply(pd.to_numeric)  # 종목코드, 종목명을 제외하고 형변환
            column_name_map = dict(zip(target_columns, output_columns))
            df.rename(columns=column_name_map, inplace=True)
            df = df[df['보유수량'] != 0]
            r2 = t1.get_body().output2
            return int(r2[0]['tot_evlu_amt']), df
        else:
            logger.info(f"t1.is_ok(): {t1.is_ok()}, output1: {output1}")
            tot_evlu_amt = 0
            if t1.is_ok():
                r2 = t1.get_body().output2
                tot_evlu_amt = int(r2[0]['tot_evlu_amt'])
            return tot_evlu_amt, pd.DataFrame(columns=output_columns)

    def get_minute_chart_data(self, stock_code):
        # 계좌 잔고 평가 잔고와 상세 내역을 DataFrame 으로 반환
        url = '/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice'
        tr_id = "FHKST03010200"

        params = {
            'FID_ETC_CLS_CODE': "",
            'FID_COND_MRKT_DIV_CODE': 'J',
            'FID_INPUT_ISCD': stock_code,
            'FID_INPUT_HOUR_1': datetime.datetime.now().strftime("%H%M%S"),
            'FID_PW_DATA_INCU_YN': 'Y',
        }

        t1 = self._url_fetch(url, tr_id, params)
        output_columns = ['일자', '시간', '시가', '고가', '저가', '종가']
        if t1 is None:
            return 0, pd.DataFrame(columns=output_columns)
        try:
            output2 = t1.get_body().output2
        except Exception as e:
            logger.info(f"Exception: {e}, t1: {t1}")
            return 0, pd.DataFrame(columns=output_columns)
        if t1 is not None and t1.is_ok() and output2:  # body 의 rt_cd 가 0 인 경우만 성공
            df = pd.DataFrame(output2)
            target_columns = [
                'stck_bsop_date',
                'stck_cntg_hour',
                'stck_oprc',
                'stck_hgpr',
                'stck_lwpr',
                'stck_prpr',
            ]
            df = df[target_columns]
            df[target_columns[2:]] = df[target_columns[2:]].apply(pd.to_numeric)  # 종목코드, 종목명을 제외하고 형변환
            column_name_map = dict(zip(target_columns, output_columns))
            df.rename(columns=column_name_map, inplace=True)
            return df[::-1].reset_index(drop=True)
        else:
            return pd.DataFrame(columns=output_columns)

    def list_conditions(self):
        url = '/uapi/domestic-stock/v1/quotations/psearch-title'
        tr_id = "HHKST03900300"

        params = {
            'user_id': self.htsid,
        }

        t1 = self._url_fetch(url, tr_id, params)
        output_columns = ['조건키값', '그룹명', '조건명']
        if t1 is None:
            return pd.DataFrame(columns=output_columns)
        try:
            output2 = t1.get_body().output2
        except Exception as e:
            logger.info(f"Exception: {e}, t1: {t1}")
            return pd.DataFrame(columns=output_columns)
        if t1 is not None and t1.is_ok() and output2:  # body 의 rt_cd 가 0 인 경우만 성공
            df = pd.DataFrame(output2)
            target_columns = [
                'seq',
                'grp_nm',
                'condition_nm',
            ]
            df = df[target_columns]
            column_name_map = dict(zip(target_columns, output_columns))
            df.rename(columns=column_name_map, inplace=True)
            return df.reset_index(drop=True)
        else:
            return pd.DataFrame(columns=output_columns)

    def list_condition_matching_stocks(self, seq_num):
        url = '/uapi/domestic-stock/v1/quotations/psearch-result'
        tr_id = "HHKST03900400"

        params = {
            'user_id': self.htsid,
            'seq': seq_num,
        }

        t1 = self._url_fetch(url, tr_id, params)
        output_columns = ['종목코드', '종목명', '현재가', '등락율']
        if t1 is None:
            return pd.DataFrame(columns=output_columns)
        try:
            output2 = t1.get_body().output2
        except Exception as e:
            logger.info(f"Exception: {e}, t1: {t1}")
            return pd.DataFrame(columns=output_columns)
        if t1 is not None and t1.is_ok() and output2:  # body 의 rt_cd 가 0 인 경우만 성공
            df = pd.DataFrame(output2)
            target_columns = [
                'code',
                'name',
                'price',
                'chgrate',
            ]
            df = df[target_columns]
            column_name_map = dict(zip(target_columns, output_columns))
            df[target_columns[2:]] = df[target_columns[2:]].apply(pd.to_numeric)  # 종목코드, 종목명을 제외하고 형변환
            df.rename(columns=column_name_map, inplace=True)
            return df.reset_index(drop=True)
        else:
            return pd.DataFrame(columns=output_columns)

    def list_overseas_condition_matching_stocks(self, exchange_code="NAS"):
        url = '/uapi/overseas-price/v1/quotations/inquire-search'
        tr_id = "HHDFS76410000"

        params = {
            'AUTH': "",
            'EXCD': exchange_code,
            'CO_YN_PRICECUR': "1",
            'CO_ST_PRICECUR': "0",
            'CO_EN_PRICECUR': "5",
            'CO_YN_RATE': "1",
            'CO_ST_RATE': "10",
            'CO_EN_RATE': "100",
        }

        t1 = self._url_fetch(url, tr_id, params)
        output_columns = ['종목코드', '종목명', '현재가', '등락율']
        if t1 is None:
            return pd.DataFrame(columns=output_columns)
        try:
            output2 = t1.get_body().output2
        except Exception as e:
            logger.info(f"Exception: {e}, t1: {t1}")
            return pd.DataFrame(columns=output_columns)
        if t1 is not None and t1.is_ok() and output2:  # body 의 rt_cd 가 0 인 경우만 성공
            df = pd.DataFrame(output2)
            target_columns = [
                'symb',
                'name',
                'last',
                'rate',
            ]
            df = df[target_columns]
            column_name_map = dict(zip(target_columns, output_columns))
            df[target_columns[2:]] = df[target_columns[2:]].apply(pd.to_numeric)  # 종목코드, 종목명을 제외하고 형변환
            df.rename(columns=column_name_map, inplace=True)
            return df.reset_index(drop=True)
        else:
            return pd.DataFrame(columns=output_columns)

    def get_hoga_info(self, stock_no):
        url = "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
        tr_id = "FHKST01010200"

        params = {
            'FID_COND_MRKT_DIV_CODE': "J",
            'FID_INPUT_ISCD': stock_no
        }

        t1 = self._url_fetch(url, tr_id, params)

        if t1 is not None and t1.is_ok():
            return t1.get_body().output1
        elif t1 is None:
            return dict()
        else:
            t1.print_error()
            return dict()

    def get_fluctuation_ranking(self):
        url = "/uapi/domestic-stock/v1/ranking/fluctuation"
        tr_id = "FHPST01700000"

        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_cond_scr_div_code": "20170",
            "fid_input_iscd": "0000",
            "fid_rank_sort_cls_code": "0",
            "fid_input_cnt_1": "0",
            "fid_prc_cls_code": "0",
            "fid_input_price_1": "",
            "fid_input_price_2": "",
            "fid_vol_cnt": "",
            "fid_trgt_cls_code": "0",
            "fid_trgt_exls_cls_code": "0",
            "fid_div_cls_code": "0",
            "fid_rsfl_rate1": "",
            "fid_rsfl_rate2": ""
        }

        t1 = self._url_fetch(url, tr_id, params)

        if t1 is not None and t1.is_ok():
            df = pd.DataFrame(t1.get_body().output)
            target_columns = ['stck_shrn_iscd', 'stck_prpr', 'prdy_ctrt']
            output_columns = ['종목코드', '현재가', '전일대비율']
            df = df[target_columns]
            column_name_map = dict(zip(target_columns, output_columns))
            return df.rename(columns=column_name_map)
        elif t1 is None:
            return dict()
        else:
            t1.print_error()
            return dict()

    def get_stock_info(self, stock_no):
        url = "/uapi/domestic-stock/v1/quotations/search-stock-info"
        tr_id = "CTPF1002R"

        params = {
            'PRDT_TYPE_CD': "300",
            'PDNO': stock_no
        }

        t1 = self._url_fetch(url, tr_id, params)

        if t1 is not None and t1.is_ok():
            return t1.get_body().output
        elif t1 is None:
            return dict()
        else:
            t1.print_error()
            return dict()

    def get_current_price(self, stock_no):
        url = "/uapi/domestic-stock/v1/quotations/inquire-price"
        tr_id = "FHKST01010100"

        params = {
            'FID_COND_MRKT_DIV_CODE': 'J',
            'FID_INPUT_ISCD': stock_no
        }

        t1 = self._url_fetch(url, tr_id, params)

        if t1 is not None and t1.is_ok():
            return t1.get_body().output
        elif t1 is None:
            return dict()
        else:
            t1.print_error()
            return dict()

    def get_overseas_ticker_info(self, exchange_code, stock_no):
        url = "/uapi/overseas-price/v1/quotations/search-info"
        tr_id = "CTPF1702R"

        exchange_code_to_cd_map = {
            "NAS": 512,
            "NYS": 513,
            "AMS": 529,
        }

        params = {
            'PRDT_TYPE_CD': exchange_code_to_cd_map[exchange_code],  # 거래소 코드
            'PDNO': stock_no,  # 종목코드
        }

        t1 = self._url_fetch(url, tr_id, params)

        if t1 is not None and t1.is_ok():
            return t1.get_body().output
        elif t1 is None:
            return dict()
        else:
            t1.print_error()
            return dict()

    def get_overseas_current_price(self, exchange_code, stock_no):
        url = "/uapi/overseas-price/v1/quotations/price"
        tr_id = "HHDFS00000300"

        params = {
            'AUTH': "",
            'EXCD': exchange_code,  # 거래소 코드
            'SYMB': stock_no,  # 종목코드
        }

        t1 = self._url_fetch(url, tr_id, params)

        if t1 is not None and t1.is_ok():
            return t1.get_body().output
        elif t1 is None:
            return dict()
        else:
            t1.print_error()
            return dict()

    def overseas_do_order(self, stock_code, exchange_code, order_qty, order_price, prd_code="01", buy_flag=True, order_type="00"):
        url = "/uapi/overseas-stock/v1/trading/order"

        if buy_flag:
            tr_id = "TTTT1002U"  # buy
            if self.is_paper_trading:
                tr_id = "VTTT1002U"  # buy
        else:
            tr_id = "TTTT1006U"  # sell
            if self.is_paper_trading:
                tr_id = "VTTT1006U"

        params = {
            'CANO': self.account_num,
            'ACNT_PRDT_CD': prd_code,
            "OVRS_EXCG_CD": exchange_code,
            'PDNO': stock_code,
            'ORD_QTY': str(order_qty),
            'OVRS_ORD_UNPR': str(order_price),
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": order_type,
        }

        t1 = self._url_fetch(url, tr_id, params, is_post_request=True, use_hash=True)

        if t1 is not None and t1.is_ok():
            return t1
        elif t1 is None:
            return None
        else:
            t1.print_error()
            return None

    def overseas_do_sell(self, stock_code, exchange_code, order_qty, order_price, prd_code="01", order_type="00"):
        t1 = self.overseas_do_order(
            stock_code,
            exchange_code,
            order_qty,
            order_price,
            prd_code=prd_code,
            buy_flag=False,
            order_type=order_type,
        )
        return t1

    def overseas_do_buy(self, stock_code, exchange_code, order_qty, order_price, prd_code="01", order_type="00"):
        t1 = self.overseas_do_order(
            stock_code,
            exchange_code,
            order_qty,
            order_price,
            prd_code=prd_code,
            buy_flag=True,
            order_type=order_type,
        )
        return t1

    def do_order(self, stock_code, order_qty, order_price, prd_code="01", buy_flag=True, order_type="00", exchange="KRX"):
        url = "/uapi/domestic-stock/v1/trading/order-cash"

        if buy_flag:
            tr_id = "TTTC0012U"  # buy
            if self.is_paper_trading:
                tr_id = "VTTC0012U"
        else:
            tr_id = "TTTC0011U"  # sell
            if self.is_paper_trading:
                tr_id = "VTTC0011U"  # sell

        params = {
            'CANO': self.account_num,
            'ACNT_PRDT_CD': prd_code,
            'PDNO': stock_code,
            'ORD_DVSN': order_type,
            'ORD_QTY': str(order_qty),
            'ORD_UNPR': str(order_price),
            'CTAC_TLNO': '',
            'SLL_TYPE': '01',
            'EXCG_ID_DVSN_CD': exchange,
        }

        t1 = self._url_fetch(url, tr_id, params, is_post_request=True, use_hash=True)

        if t1 is not None and t1.is_ok():
            return t1
        elif t1 is None:
            return None
        else:
            t1.print_error()
            return None

    def do_sell(self, stock_code, order_qty, order_price, order_type="00"):
        t1 = self.do_order(stock_code, order_qty, order_price, buy_flag=False, order_type=order_type)
        return t1

    def do_buy(self, stock_code, order_qty, order_price, order_type="00"):
        t1 = self.do_order(stock_code, order_qty, order_price, buy_flag=True, order_type=order_type)
        return t1

    def get_overseas_orders(self, prd_code='01', exchange_code='') -> pd.DataFrame:
        url = "/uapi/overseas-stock/v1/trading/inquire-nccs"
        tr_id = "TTTS3018R"
        params = {
            "CANO": self.account_num,
            "ACNT_PRDT_CD": prd_code,
            "OVRS_EXCG_CD": exchange_code,
            "SORT_SQN": "DS",
            "CTX_AREA_FK200": '',
            "CTX_AREA_NK200": '',
        }

        t1 = self._url_fetch(url, tr_id, params)
        if t1 is not None and t1.is_ok() and t1.get_body().output:
            tdf = pd.DataFrame(t1.get_body().output)
            tdf.set_index('odno', inplace=True)
            cf1 = ['pdno', 'ft_ord_qty', 'ft_ord_unpr3', 'ord_tmd', 'ovrs_excg_cd', 'orgn_odno', 'nccs_qty', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name']
            cf2 = ['종목코드', '주문수량', '주문가격', '시간', '거래소코드', '원주문번호', '주문가능수량', '매도매수구분코드', '매도매수구분코드명']
            tdf = tdf[cf1]
            ren_dict = dict(zip(cf1, cf2))

            return tdf.rename(columns=ren_dict)
        else:
            return None

    def get_overseas_finished_orders(self, prd_code='01') -> pd.DataFrame:
        url = "/uapi/overseas-stock/v1/trading/inquire-ccnl"
        tr_id = "TTTS3035R"
        params = {
            "CANO": self.account_num,
            "ACNT_PRDT_CD": prd_code,
            "PDNO": "%",
            "ORD_STRT_DT": datetime.datetime.now().strftime("%Y%m%d"),
            "ORD_END_DT": datetime.datetime.now().strftime("%Y%m%d"),
            "SLL_BUY_DVSN": "00",
            "CCLD_NCCS_DVSN": "01",
            "OVRS_EXCG_CD": "NASD",
            "SORT_SQN": "AS",
            "ORD_DT": "",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "CTX_AREA_FK200": '',
            "CTX_AREA_NK200": '',
        }

        t1 = self._url_fetch(url, tr_id, params)
        if t1 is not None and t1.is_ok() and t1.get_body().output:
            tdf = pd.DataFrame(t1.get_body().output)
            tdf.set_index('odno', inplace=True)
            cf1 = ['pdno', 'ft_ord_qty', 'ft_ord_unpr3', 'ft_ccld_unpr3', 'ft_ccld_qty', 'ord_tmd', 'orgn_odno', 'nccs_qty', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name']
            cf2 = ['종목코드', '주문수량', '주문가격', '체결가격', '체결수량', '시간', '원주문번호', '주문가능수량', '매도매수구분코드', '매도매수구분코드명']
            tdf = tdf[cf1]
            ren_dict = dict(zip(cf1, cf2))

            return tdf.rename(columns=ren_dict)
        else:
            return None

    def get_orders(self, prd_code='01') -> pd.DataFrame:
        url = "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"
        tr_id = "TTTC8036R"
        params = {
            "CANO": self.account_num,
            "ACNT_PRDT_CD": prd_code,
            "CTX_AREA_FK100": '',
            "CTX_AREA_NK100": '',
            "INQR_DVSN_1": '0',
            "INQR_DVSN_2": '0'
        }

        t1 = self._url_fetch(url, tr_id, params)
        if t1 is not None and t1.is_ok() and t1.get_body().output:
            tdf = pd.DataFrame(t1.get_body().output)
            tdf.set_index('odno', inplace=True)
            cf1 = ['pdno', 'ord_qty', 'ord_unpr', 'ord_tmd', 'ord_gno_brno', 'orgn_odno', 'psbl_qty']
            cf2 = ['종목코드', '주문수량', '주문가격', '시간', '주문점', '원주문번호', '주문가능수량']
            tdf = tdf[cf1]
            ren_dict = dict(zip(cf1, cf2))

            return tdf.rename(columns=ren_dict)
        else:
            return None

    def _do_cancel_revise(self, order_no, order_branch, order_qty, order_price, prd_code, order_dv, cncl_dv, qty_all_yn, exchange="KRX"):
        # 특정 주문 취소(01)/정정(02)
        # Input: 주문 번호(get_orders 를 호출하여 얻은 DataFrame 의 index  column 값이 취소 가능한 주문번호임)
        #       주문점(통상 06010), 주문수량, 주문가격, 상품코드(01), 주문유형(00), 정정구분(취소-02, 정정-01)
        # Output: APIResponse object
        url = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
        tr_id = "TTTC0013U"

        params = {
            "CANO": self.account_num,
            "ACNT_PRDT_CD": prd_code,
            "KRX_FWDG_ORD_ORGNO": order_branch,
            "ORGN_ODNO": order_no,
            "ORD_DVSN": order_dv,
            "RVSE_CNCL_DVSN_CD": cncl_dv,  # 취소(02)
            "ORD_QTY": str(order_qty),
            "ORD_UNPR": str(order_price),
            "QTY_ALL_ORD_YN": qty_all_yn,
            "EXCG_ID_DVSN_CD": exchange,
        }

        t1 = self._url_fetch(url, tr_id, params=params, is_post_request=True)

        if t1 is not None and t1.is_ok():
            return t1
        elif t1 is None:
            return None
        else:
            t1.print_error()
            return None

    def _overseas_do_cancel_revise(self, order_no, stock_code, order_branch, order_qty, order_price, prd_code, cncl_dv):
        # 특정 주문 취소(01)/정정(02)
        # Input: 주문 번호(get_orders 를 호출하여 얻은 DataFrame 의 index  column 값이 취소 가능한 주문번호임)
        #       주문점(통상 06010), 주문수량, 주문가격, 상품코드(01), 주문유형(00), 정정구분(취소-02, 정정-01)
        # Output: APIResponse object
        url = "/uapi/overseas-stock/v1/trading/order-rvsecncl"
        tr_id = "TTTT1004U"

        params = {
            "CANO": self.account_num,
            "ACNT_PRDT_CD": prd_code,
            "OVRS_EXCG_CD": order_branch,
            "PDNO": stock_code,
            "ORGN_ODNO": order_no,
            "ORD_SVR_DVSN_CD": "0",
            "RVSE_CNCL_DVSN_CD": cncl_dv,  # 취소(02)
            "ORD_QTY": str(order_qty),
            "OVRS_ORD_UNPR": str(order_price),
        }

        t1 = self._url_fetch(url, tr_id, params=params, is_post_request=True)

        if t1 is not None and t1.is_ok():
            return t1
        elif t1 is None:
            return None
        else:
            t1.print_error()
            return None

    def do_cancel(self, order_no, order_qty, order_price="01", order_branch='06010', prd_code='01', order_dv='00', cncl_dv='02', qty_all_yn="Y"):
        return self._do_cancel_revise(order_no, order_branch, order_qty, order_price, prd_code, order_dv, cncl_dv, qty_all_yn)

    def overseas_do_cancel(self, order_no, stock_code, order_qty, order_price="0", order_branch='06010', prd_code='01', cncl_dv='02'):
        return self._overseas_do_cancel_revise(order_no, stock_code, order_branch, order_qty, order_price, prd_code, cncl_dv)

    def do_revise(self, order_no, order_qty, order_price, order_branch='06010', prd_code='01', order_dv='00', cncl_dv='01', qty_all_yn="Y"):
        return self._do_cancel_revise(order_no, order_branch, order_qty, order_price, prd_code, order_dv, cncl_dv, qty_all_yn)

    def overseas_do_revise(self, order_no, stock_code, order_qty, order_price="0", order_branch='06010', prd_code='01', cncl_dv='01'):
        return self._overseas_do_cancel_revise(order_no, stock_code, order_branch, order_qty, order_price, prd_code, cncl_dv)

    def overseas_do_cancel_all(self, skip_codes=[]):
        tdf = self.get_overseas_orders()
        if tdf is not None:
            for order_num, row in tdf.iterrows():
                stock_code = row["종목코드"]
                if stock_code in skip_codes:
                    continue
                exchange = row["거래소코드"]
                price = row["주문가격"]
                qty = row["주문수량"]
                ar = self.overseas_do_cancel(order_num, stock_code, qty, price, exchange)
                logger.info(f"get_error_code: {ar.get_error_code()}, get_error_message: {ar.get_error_message()}")
                time.sleep(0.02)

    def do_cancel_all(self, skip_codes=[]):
        tdf = self.get_orders()
        if tdf is not None:
            for order_num, row in tdf.iterrows():
                stock_code = row["종목코드"]
                if stock_code in skip_codes:
                    continue
                branch = row["주문점"]
                price = row["주문가격"]
                qty = row["주문수량"]
                ar = self.do_cancel(order_num, qty, price, branch)
                logger.info(f"get_error_code: {ar.get_error_code()}, get_error_message: {ar.get_error_message()}")
                time.sleep(0.02)

    def get_my_complete(self, sdt, edt=None, prd_code='01', zipFlag=True):
        # 내 계좌의 일별 주문 체결 조회
        # Input: 시작일, 종료일 (Option)지정하지 않으면 현재일
        # output: DataFrame
        url = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        tr_id = "TTTC8001R"

        if (edt is None):
            ltdt = datetime.datetime.now().strftime('%Y%m%d')
        else:
            ltdt = edt

        params = {
            "CANO": self.account_num,
            "ACNT_PRDT_CD": prd_code,
            "INQR_STRT_DT": sdt,
            "INQR_END_DT": ltdt,
            "SLL_BUY_DVSN_CD": '00',
            "INQR_DVSN": '00',
            "PDNO": "",
            "CCLD_DVSN": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "",
            "INQR_DVSN_2": "",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }

        t1 = self._url_fetch(url, tr_id, params)

        # output1 과 output2 로 나뉘어서 결과가 옴. 지금은 output1만 DF 로 변환
        if t1 is not None and t1.is_ok():
            tdf = pd.DataFrame(t1.get_body().output1)
            tdf.set_index('odno', inplace=True)
            if (zipFlag):
                return tdf[
                    [
                        'ord_dt', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno',
                        'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn',
                        'tot_ccld_amt', 'rmn_qty',
                    ]
                ]
            else:
                return tdf
        elif t1 is None:
            return pd.DataFrame()
        else:
            t1.print_error()
            return pd.DataFrame()

    def get_buyable_cash(self, stock_code='', qry_price=0, prd_code='01'):
        url = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        tr_id = "TTTC8908R"

        params = {
            "CANO": self.account_num,
            "ACNT_PRDT_CD": prd_code,
            "PDNO": stock_code,
            "ORD_UNPR": str(qry_price),
            "ORD_DVSN": "02",
            "CMA_EVLU_AMT_ICLD_YN": "Y",  # API 설명부분 수정 필요 (YN)
            "OVRS_ICLD_YN": "N"
        }

        t1 = self._url_fetch(url, tr_id, params)

        if t1 is not None and t1.is_ok():
            return int(t1.get_body().output['ord_psbl_cash'])
        elif t1 is None:
            return 0
        else:
            t1.print_error()
            return 0

    def get_stock_completed(self, stock_no):
        # 종목별 체결 Data
        # Input: 종목코드
        # Output: 체결 Data DataFrame
        # 주식체결시간, 주식현재가, 전일대비, 전일대비부호, 체결거래량, 당일 체결강도, 전일대비율
        url = "/uapi/domestic-stock/v1/quotations/inquire-ccnl"

        tr_id = "FHKST01010300"

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_no
        }

        t1 = self._url_fetch(url, tr_id, params)

        if t1 is not None and t1.is_ok():
            return pd.DataFrame(t1.get_body().output)
        elif t1 is None:
            return pd.DataFrame()
        else:
            t1.print_error()
            return pd.DataFrame()

    def get_stock_history(self, stock_no, gb_cd='D'):
        # 종목별 history data (현재 기준 30개만 조회 가능)
        # Input: 종목코드, 구분(D, W, M 기본값은 D)
        # output: 시세 History DataFrame
        url = "/uapi/domestic-stock/v1/quotations/inquire-daily-price"
        tr_id = "FHKST01010400"

        params = {
            "FID_COND_MRKT_DIV_CODE": 'J',
            "FID_INPUT_ISCD": stock_no,
            "FID_PERIOD_DIV_CODE": gb_cd,
            "FID_ORG_ADJ_PRC": "0000000001"
        }

        t1 = self._url_fetch(url, tr_id, params)

        if t1 is not None and t1.is_ok():
            return pd.DataFrame(t1.get_body().output)
        elif t1 is None:
            return pd.DataFrame()
        else:
            t1.print_error()
            return pd.DataFrame()

    def get_stock_history_by_ohlcv(self, stock_no, gb_cd='D', adVar=False):
        # 종목별 history data 를 표준 OHLCV DataFrame 으로 반환
        # Input: 종목코드, 구분(D, W, M 기본값은 D), (Option)adVar 을 True 로 설정하면
        #        OHLCV 외에 inter_volatile 과 pct_change 를 추가로 반환한다.
        # output: 시세 History OHLCV DataFrame
        hdf1 = self.get_stock_history(stock_no, gb_cd)

        chosend_fld = ['stck_bsop_date', 'stck_oprc', 'stck_hgpr', 'stck_lwpr', 'stck_clpr', 'acml_vol']
        renamed_fld = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

        hdf1 = hdf1[chosend_fld]
        ren_dict = dict()
        i = 0
        for x in chosend_fld:
            ren_dict[x] = renamed_fld[i]
            i += 1

        hdf1.rename(columns=ren_dict, inplace=True)
        hdf1[['Date']] = hdf1[['Date']].apply(pd.to_datetime)
        hdf1[['Open', 'High', 'Low', 'Close', 'Volume']] = hdf1[['Open', 'High', 'Low', 'Close', 'Volume']].apply(
            pd.to_numeric)
        hdf1.set_index('Date', inplace=True)

        if (adVar):
            hdf1['inter_volatile'] = (hdf1['High'] - hdf1['Low']) / hdf1['Close']
            hdf1['pct_change'] = (hdf1['Close'] - hdf1['Close'].shift(-1)) / hdf1['Close'].shift(-1) * 100

        return hdf1

    def get_stock_investor(self, stock_no):
        # 투자자별 매매 동향
        # Input: 종목코드
        # output: 매매 동향 History DataFrame (Date, PerBuy, ForBuy, OrgBuy) 30개 row를 반환
        url = "/uapi/domestic-stock/v1/quotations/inquire-investor"
        tr_id = "FHKST01010900"

        params = {
            "FID_COND_MRKT_DIV_CODE": 'J',
            "FID_INPUT_ISCD": stock_no
        }

        t1 = self._url_fetch(url, tr_id, params)

        if t1 is not None and t1.is_ok():
            hdf1 = pd.DataFrame(t1.get_body().output)

            chosend_fld = ['stck_bsop_date', 'prsn_ntby_qty', 'frgn_ntby_qty', 'orgn_ntby_qty']
            renamed_fld = ['Date', 'PerBuy', 'ForBuy', 'OrgBuy']

            hdf1 = hdf1[chosend_fld]
            ren_dict = dict()
            i = 0
            for x in chosend_fld:
                ren_dict[x] = renamed_fld[i]
                i += 1

            hdf1.rename(columns=ren_dict, inplace=True)
            hdf1[['Date']] = hdf1[['Date']].apply(pd.to_datetime)
            hdf1[['PerBuy', 'ForBuy', 'OrgBuy']] = hdf1[['PerBuy', 'ForBuy', 'OrgBuy']].apply(pd.to_numeric)
            hdf1['EtcBuy'] = (hdf1['PerBuy'] + hdf1['ForBuy'] + hdf1['OrgBuy']) * -1
            hdf1.set_index('Date', inplace=True)
            # sum을 맨 마지막에 추가하는 경우
            # tdf.append(tdf.sum(numeric_only=True), ignore_index=True) <- index를 없애고  만드는 경우
            # tdf.loc['Total'] = tdf.sum() <- index 에 Total 을 추가하는 경우
            return hdf1
        elif t1 is None:
            return pd.DataFrame()
        else:
            t1.print_error()
            return pd.DataFrame()

    def overseas_get_send_data(self, cmd=None, stockcode=None):
        # 1.주식호가, 2.주식호가해제, 3.주식체결, 4.주식체결해제, 5.주식체결통보(고객), 6.주식체결통보해제(고객), 7.주식체결통보(모의), 8.주식체결통보해제(모의)
        # 입력값 체크 step
        assert 0 < cmd < 9, f"Wrong Input Data: {cmd}"

        # 입력값에 따라 전송 데이터셋 구분 처리
        if cmd == 1:  # 주식호가 등록
            tr_id = 'HDFSASP0'
            tr_type = '1'
        elif cmd == 2:  # 주식호가 등록해제
            tr_id = 'HDFSASP0'
            tr_type = '2'
        elif cmd == 3:  # 주식체결 등록
            tr_id = 'HDFSCNT0'
            tr_type = '1'
        elif cmd == 4:  # 주식체결 등록해제
            tr_id = 'HDFSCNT0'
            tr_type = '2'
        elif cmd == 5:  # 주식체결통보 등록(고객용)
            tr_id = 'H0GSCNI0'  # 고객체결통보
            tr_type = '1'
        elif cmd == 6:  # 주식체결통보 등록해제(고객용)
            tr_id = 'H0GSCNI0'  # 고객체결통보
            tr_type = '2'

        # send json, 체결통보는 tr_key 입력항목이 상이하므로 분리를 한다.
        if cmd in (5, 6, 7, 8):
            senddata = '{"header":{"approval_key":"' + self.websocket_approval_key + '","custtype":"' + self.custtype + '","tr_type":"' + tr_type + '","content-type":"utf-8"},"body":{"input":{"tr_id":"' + tr_id + '","tr_key":"' + self.htsid + '"}}}'
        else:
            senddata = '{"header":{"approval_key":"' + self.websocket_approval_key + '","custtype":"' + self.custtype + '","tr_type":"' + tr_type + '","content-type":"utf-8"},"body":{"input":{"tr_id":"' + tr_id + '","tr_key":"' + stockcode + '"}}}'
        return senddata

    def get_send_data(self, cmd=None, stockcode=None):
        # 1.주식호가, 2.주식호가해제, 3.주식체결, 4.주식체결해제, 5.주식체결통보(고객), 6.주식체결통보해제(고객), 7.주식체결통보(모의), 8.주식체결통보해제(모의)
        # 입력값 체크 step
        assert 0 < cmd < 9, f"Wrong Input Data: {cmd}"

        # 입력값에 따라 전송 데이터셋 구분 처리
        if cmd == 1:  # 주식호가 등록
            tr_id = 'H0UNASP0'
            tr_type = '1'
        elif cmd == 2:  # 주식호가 등록해제
            tr_id = 'H0UNASP0'
            tr_type = '2'
        elif cmd == 3:  # 주식체결 등록
            tr_id = 'H0UNCNT0'
            tr_type = '1'
        elif cmd == 4:  # 주식체결 등록해제
            tr_id = 'H0UNCNT0'
            tr_type = '2'
        elif cmd == 5:  # 주식체결통보 등록(고객용)
            tr_id = 'H0STCNI0'  # 고객체결통보
            tr_type = '1'
        elif cmd == 6:  # 주식체결통보 등록해제(고객용)
            tr_id = 'H0STCNI0'  # 고객체결통보
            tr_type = '2'
        elif cmd == 7:  # 주식체결통보 등록(모의)
            tr_id = 'H0STCNI9'  # 테스트용 직원체결통보
            tr_type = '1'
        elif cmd == 8:  # 주식체결통보 등록해제(모의)
            tr_id = 'H0STCNI9'  # 테스트용 직원체결통보
            tr_type = '2'

        # send json, 체결통보는 tr_key 입력항목이 상이하므로 분리를 한다.
        if cmd in (5, 6, 7, 8):
            senddata = '{"header":{"approval_key":"' + self.websocket_approval_key + '","custtype":"' + self.custtype + '","tr_type":"' + tr_type + '","content-type":"utf-8"},"body":{"input":{"tr_id":"' + tr_id + '","tr_key":"' + self.htsid + '"}}}'
        else:
            senddata = '{"header":{"approval_key":"' + self.websocket_approval_key + '","custtype":"' + self.custtype + '","tr_type":"' + tr_type + '","content-type":"utf-8"},"body":{"input":{"tr_id":"' + tr_id + '","tr_key":"' + stockcode + '"}}}'
        return senddata
    
    def future_options_do_amend_cancel_order(self, order_qty, order_price=0, order_num='', is_cancel_order=True, prd_code="03", order_type="04"):
        url = "/uapi/domestic-futureoption/v1/trading/order-rvsecncl"
        if self.is_paper_trading:
            tr_id = "VTTO1103U"
        else:
            tr_id = "TTTO1103U"

        params = {
            'ORD_PRCS_DVSN_CD': "02",
            'CANO': self.future_account_num,
            'ACNT_PRDT_CD': prd_code,
            'RVSE_CNCL_DVSN_CD': "02" if is_cancel_order else "01",
            'ORGN_ODNO': order_num,
            'ORD_QTY': str(order_qty),
            'UNIT_PRICE': str(order_price),
            "NMPR_TYPE_CD": order_type,
            "KRX_NMPR_CNDT_CD": "0",
            'RMN_QTY_YN': 'Y',
            "ORD_DVSN_CD": order_type,
        }

        t1 = self._url_fetch(url, tr_id, params, is_post_request=True)

        if t1 is not None and t1.is_ok():
            return t1
        elif t1 is None:
            return None
        else:
            t1.print_error()
            return None

    def future_options_do_order(self, product_code, order_qty, order_price=0, is_buy_order=True, prd_code="03", order_type="04"):
        url = "/uapi/domestic-futureoption/v1/trading/order"
        if self.is_paper_trading:
            tr_id = "VTTO1101U"
        else:
            tr_id = "TTTO1101U"

        params = {
            'ORD_PRCS_DVSN_CD': "02",
            'CANO': self.future_account_num,
            'ACNT_PRDT_CD': prd_code,
            "SLL_BUY_DVSN_CD": "02" if is_buy_order else "01",
            'SHTN_PDNO': product_code,
            'ORD_QTY': str(order_qty),
            'UNIT_PRICE': str(order_price),
            "NMPR_TYPE_CD": order_type,
            "KRX_NMPR_CNDT_CD": "0",
            "ORD_DVSN_CD": order_type,
        }

        t1 = self._url_fetch(url, tr_id, params, is_post_request=True)

        if t1 is not None and t1.is_ok():
            return t1
        elif t1 is None:
            return None
        else:
            t1.print_error()
            return None

    def get_futures_price(self, future_code):
        url = "/uapi/domestic-futureoption/v1/quotations/inquire-price"
        tr_id = "FHMIF10000000"

        params = {
            "FID_COND_MRKT_DIV_CODE": "F",
            "FID_INPUT_ISCD": future_code,
        }

        t1 = self._url_fetch(url, tr_id, params, is_post_request=False)

        if t1 is not None and t1.is_ok():
            return float(t1.get_body().output1['futs_prpr'])
        elif t1 is None:
            return None
        else:
            t1.print_error()
            return None

    def get_futures_open_price(self, future_code):
        url = "/uapi/domestic-futureoption/v1/quotations/inquire-price"
        tr_id = "FHMIF10000000"

        params = {
            "FID_COND_MRKT_DIV_CODE": "F",
            "FID_INPUT_ISCD": future_code,
        }

        t1 = self._url_fetch(url, tr_id, params, is_post_request=False)

        if t1 is not None and t1.is_ok():
            return float(t1.get_body().output1['futs_oprc'])
        elif t1 is None:
            return None
        else:
            t1.print_error()
            return None

    def get_future_option_orders(self):
        url = "/uapi/domestic-futureoption/v1/trading/inquire-ccnl"
        if self.is_paper_trading:
            tr_id = "VTTO5201R"
        else:
            tr_id = "TTTO5201R"

        params = {
            'CANO': self.future_account_num,
            'ACNT_PRDT_CD': '03',
            'STRT_ORD_DT': datetime.datetime.now().strftime("%Y%m%d"),
            'END_ORD_DT': datetime.datetime.now().strftime("%Y%m%d"),
            'SLL_BUY_DVSN_CD': "00",
            'CCLD_NCCS_DVSN': '02',
            'SORT_SQN': 'DS',
            'STRT_ODNO': '0',
            'PDNO': '',
            'MKET_ID_CD': '',
            'CTX_AREA_FK200': '',
            'CTX_AREA_NK200': '',
        }

        t1 = self._url_fetch(url, tr_id, params, is_post_request=False)

        if t1 is not None and t1.is_ok():
            try:
                df = pd.DataFrame(t1.get_body().output1)[['pdno', 'prdt_name', 'ord_qty', 'qty', 'odno', 'trad_dvsn_name', 'nmpr_type_name']]
                df.rename(
                    columns={
                        'pdno': '종목코드',
                        'prdt_name': '종목명',
                        'ord_qty': '주문수량',
                        'qty': '미체결수량',
                        'odno': '주문번호',
                        'trad_dvsn_name': '매수매도구분',
                        'nmpr_type_name': '주문유형',
                    },
                    inplace=True,
                )
                df = df[df['매수매도구분'].isin(['매수', '매도'])]
                return df
            except:
                return pd.DataFrame(columns=['종목코드', '종목명', '주문수량', '미체결수량', '주문번호', '매수매도구분', '주문유형'])
        elif t1 is None:
            return None
        else:
            t1.print_error()
            return None

    def get_future_option_balance(self):
        url = "/uapi/domestic-futureoption/v1/trading/inquire-balance"
        if self.is_paper_trading:
            tr_id = "VTFO6118R"
        else:
            tr_id = "CTFO6118R"

        params = {
            'CANO': self.future_account_num,
            'ACNT_PRDT_CD': '03',
            'MGNA_DVSN': "01",
            'EXCC_STAT_CD': '1',
            'CTX_AREA_FK200': '',
            'CTX_AREA_NK200': '',
        }

        t1 = self._url_fetch(url, tr_id, params, is_post_request=False)

        if t1 is not None and t1.is_ok():
            추정예탁자산 = int(t1.get_body().output2['prsm_dpast'])
            매매손익금액 = int(t1.get_body().output2['trad_pfls_amt_smtl'])
            평가손익금액 = int(t1.get_body().output2['evlu_pfls_amt_smtl'])
            return dict(매매손익금액=매매손익금액, 추정예탁자산=추정예탁자산, 평가손익금액=평가손익금액)
        elif t1 is None:
            return None
        else:
            t1.print_error()
            return None

    def display_options(self, is_mini=False, target_date='202408'):
        url = "/uapi/domestic-futureoption/v1/quotations/display-board-callput"
        tr_id = "FHPIF05030100"

        params = {
            "FID_COND_MRKT_DIV_CODE": "O",
            "FID_COND_SCR_DIV_CODE": "20503",
            "FID_MRKT_CLS_CODE": "CO",
            "FID_MTRT_CNT": target_date,
            "FID_COND_MRKT_CLS_CODE": "MKI" if is_mini else "",
            "FID_MRKT_CLS_CODE1": "PO",
        }

        t1 = self._url_fetch(url, tr_id, params, is_post_request=False)

        if t1 is not None and t1.is_ok():
            hdf1 = pd.DataFrame(t1.get_body().output1)[['optn_shrn_iscd', 'acml_vol', 'optn_prdy_vrss', 'optn_prdy_ctrt', 'optn_prpr', 'acpr']]
            hdf1.rename(
                columns={
                    'optn_shrn_iscd': '종목코드',
                    'acml_vol': '거래량',
                    'optn_prdy_vrss': '전일대비',
                    'optn_prdy_ctrt': '등락율',
                    'optn_prpr': '현재가',
                    'acpr': '행사가',
                },
                inplace=True
            )
            for col in hdf1.columns:
                if col == '종목코드':
                    hdf1[col] = hdf1[col].astype(str)
                elif col == '거래량':
                    hdf1[col] = hdf1[col].astype(int)
                else:
                    hdf1[col] = hdf1[col].astype(float)
            hdf2 = pd.DataFrame(t1.get_body().output2)[['optn_shrn_iscd', 'acml_vol', 'optn_prdy_vrss', 'optn_prdy_ctrt', 'optn_prpr', 'acpr']]
            hdf2.rename(
                columns={
                    'optn_shrn_iscd': '종목코드',
                    'acml_vol': '거래량',
                    'optn_prdy_vrss': '전일대비',
                    'optn_prdy_ctrt': '등락율',
                    'optn_prpr': '현재가',
                    'acpr': '행사가',
                },
                inplace=True
            )
            for col in hdf2.columns:
                if col == '종목코드':
                    hdf2[col] = hdf2[col].astype(str)
                elif col == '거래량':
                    hdf2[col] = hdf2[col].astype(int)
                else:
                    hdf2[col] = hdf2[col].astype(float)
            hdf2 = hdf2[hdf2.columns[::-1]]
            # hdf1과 hdf2를 'acpr'를 기준으로 내부 조인
            merged_df = pd.merge(hdf1, hdf2, on='행사가', suffixes=('_콜', '_풋'))
            # 'acpr'를 중간으로 이동시키는 방법
            cols = list(merged_df.columns)
            # 'acpr' 컬럼을 제거
            cols.remove('행사가')
            # 새로운 순서 설정: 'acpr'를 중간에 위치
            new_col_order = cols[:len(cols) // 2] + ['행사가'] + cols[len(cols) // 2:]
            # 컬럼 순서 적용
            merged_df = merged_df[new_col_order]
            return merged_df
        elif t1 is None:
            return pd.DataFrame(), pd.DataFrame()
        else:
            t1.print_error()
            return pd.DataFrame(), pd.DataFrame()

    def display_weekly_options(self, is_monday=False, target_date='240801'):
        url = "/uapi/domestic-futureoption/v1/quotations/display-board-callput"
        tr_id = "FHPIF05030100"

        params = {
            "FID_COND_MRKT_DIV_CODE": "O",
            "FID_COND_SCR_DIV_CODE": "20503",
            "FID_MRKT_CLS_CODE": "CO",
            "FID_MTRT_CNT": target_date,
            "FID_COND_MRKT_CLS_CODE": "WKM" if is_monday else "WKI",
            "FID_MRKT_CLS_CODE1": "PO",
        }

        t1 = self._url_fetch(url, tr_id, params, is_post_request=False)

        if t1 is not None and t1.is_ok():
            hdf1 = pd.DataFrame(t1.get_body().output1)[['optn_shrn_iscd', 'acml_vol', 'optn_prdy_vrss', 'optn_prdy_ctrt', 'optn_prpr', 'acpr']]
            hdf1.rename(
                columns={
                    'optn_shrn_iscd': '종목코드',
                    'acml_vol': '거래량',
                    'optn_prdy_vrss': '전일대비',
                    'optn_prdy_ctrt': '등락율',
                    'optn_prpr': '현재가',
                    'acpr': '행사가',
                },
                inplace=True
            )
            for col in hdf1.columns:
                if col == '종목코드':
                    hdf1[col] = hdf1[col].astype(str)
                elif col == '거래량':
                    hdf1[col] = hdf1[col].astype(int)
                else:
                    hdf1[col] = hdf1[col].astype(float)
            hdf2 = pd.DataFrame(t1.get_body().output2)[['optn_shrn_iscd', 'acml_vol', 'optn_prdy_vrss', 'optn_prdy_ctrt', 'optn_prpr', 'acpr']]
            hdf2.rename(
                columns={
                    'optn_shrn_iscd': '종목코드',
                    'acml_vol': '거래량',
                    'optn_prdy_vrss': '전일대비',
                    'optn_prdy_ctrt': '등락율',
                    'optn_prpr': '현재가',
                    'acpr': '행사가',
                },
                inplace=True
            )
            for col in hdf2.columns:
                if col == '종목코드':
                    hdf2[col] = hdf2[col].astype(str)
                elif col == '거래량':
                    hdf2[col] = hdf2[col].astype(int)
                else:
                    hdf2[col] = hdf2[col].astype(float)
            hdf2 = hdf2[hdf2.columns[::-1]]
            # hdf1과 hdf2를 'acpr'를 기준으로 내부 조인
            merged_df = pd.merge(hdf1, hdf2, on='행사가', suffixes=('_콜', '_풋'))
            # 'acpr'를 중간으로 이동시키는 방법
            cols = list(merged_df.columns)
            # 'acpr' 컬럼을 제거
            cols.remove('행사가')
            # 새로운 순서 설정: 'acpr'를 중간에 위치
            new_col_order = cols[:len(cols) // 2] + ['행사가'] + cols[len(cols) // 2:]
            # 컬럼 순서 적용
            merged_df = merged_df[new_col_order]
            return merged_df
        elif t1 is None:
            return pd.DataFrame(), pd.DataFrame()
        else:
            t1.print_error()
            return pd.DataFrame(), pd.DataFrame()

    def get_future_options_send_data(self, cmd=None, stockcode=None):
        # 1.주식호가, 2.주식호가해제, 3.주식체결, 4.주식체결해제, 5.주식체결통보(고객), 6.주식체결통보해제(고객), 7.주식체결통보(모의), 8.주식체결통보해제(모의)
        # 입력값 체크 step
        assert 0 <= cmd < 9, f"Wrong Input Data: {cmd}"

        # 입력값에 따라 전송 데이터셋 구분 처리
        if cmd == 0:  # 지수옵션체결 등록
            tr_id = 'H0IOCNT0'
            tr_type = '2'
        elif cmd == 1:  # 지수옵션체결 등록
            tr_id = 'H0IOCNT0'
            tr_type = '1'
        elif cmd == 2:  # 지수옵션호가 등록
            tr_id = 'H0IOASP0'
            tr_type = '1'
        elif cmd == 3:  # 지수선물체결 등록
            tr_id = 'H0IFCNT0'
            tr_type = '1'
        elif cmd == 4:  # 지수옵션호가 해제
            tr_id = 'H0IOASP0'
            tr_type = '2'
        elif cmd == 5:  # 주식체결통보 등록(고객용)
            tr_id = 'H0IFCNI0'  # 고객체결통보
            tr_type = '1'
        elif cmd == 6:  # 주식체결통보 등록해제(고객용)
            tr_id = 'H0IFCNI0'  # 고객체결통보
            tr_type = '2'
        elif cmd == 7:  # 주식체결통보 등록(모의)
            tr_id = 'H0IFCNI9'  # 고객체결통보
            tr_type = '1'
        elif cmd == 8:  # 주식체결통보 등록해제(모의)
            tr_id = 'H0IFCNI9'  # 고객체결통보
            tr_type = '2'

        # send json, 체결통보는 tr_key 입력항목이 상이하므로 분리를 한다.
        if cmd in (5, 6, 7, 8):
            senddata = '{"header":{"approval_key":"' + self.g_approval_key + '","custtype":"' + self.custtype + '","tr_type":"' + tr_type + '","content-type":"utf-8"},"body":{"input":{"tr_id":"' + tr_id + '","tr_key":"' + self.htsid + '"}}}'
        else:
            senddata = '{"header":{"approval_key":"' + self.g_approval_key + '","custtype":"' + self.custtype + '","tr_type":"' + tr_type + '","content-type":"utf-8"},"body":{"input":{"tr_id":"' + tr_id + '","tr_key":"' + stockcode + '"}}}'
        return senddata



class APIResponse:
    def __init__(self, resp):
        self._rescode = resp.status_code
        self._resp = resp
        self._header = self._set_header()
        self._body = self._set_body()
        self._err_code = self._body.rt_cd
        self._err_message = self._body.msg1

    def get_result_code(self):
        return self._rescode

    def _set_header(self):
        fld = dict()
        for x in self._resp.headers.keys():
            if x.islower():
                fld[x] = self._resp.headers.get(x)
        _th_ = namedtuple('header', fld.keys())
        return _th_(**fld)

    def _set_body(self):
        _tb_ = namedtuple('body', self._resp.json().keys())
        return _tb_(**self._resp.json())

    def get_header(self):
        return self._header

    def get_body(self):
        return self._body

    def get_response(self):
        return self._resp

    def is_ok(self):
        try:
            if (self.get_body().rt_cd == '0'):
                return True
            else:
                return False
        except:
            return False

    def get_error_code(self):
        return self._err_code

    def get_error_message(self):
        return self._err_message

    def print_all(self):
        logger.info("<Header>")
        for x in self.get_header()._fields:
            logger.info(f'\t-{x}: {getattr(self.get_header(), x)}')
        logger.info("<Body>")
        for x in self.get_body()._fields:
            logger.info(f'\t-{x}: {getattr(self.get_body(), x)}')

    def print_error(self):
        logger.info(f'------------------------------')
        logger.info(f'Error in response: {self.get_result_code()}')
        logger.info(f'{self.get_body().rt_cd}, {self.get_error_code()}, {self.get_error_message()}')
        logger.info('-------------------------------')


# AES256 DECODE
def aes_cbc_base64_dec(key, iv, cipher_text):
    """
    :param key:  str type AES256 secret key value
    :param iv: str type AES256 Initialize Vector
    :param cipher_text: Base64 encoded AES256 str
    :return: Base64-AES256 decodec str
    """
    cipher = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv.encode('utf-8'))
    return bytes.decode(unpad(cipher.decrypt(b64decode(cipher_text)), AES.block_size))


if __name__ == "__main__":
    with open("./config.yaml", encoding='UTF-8') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
    env_cls = KoreaInvestEnv(cfg)
    base_headers = env_cls.get_base_headers()
    cfg = env_cls.get_full_config()
    korea_invest_api = KoreaInvestAPI(cfg, base_headers=base_headers)
    df = korea_invest_api.get_minute_chart_data("005930")
    print(df)