import requests, shutil, time, cv2
from bs4 import BeautifulSoup
import numpy as np
from keras.models import load_model
from preprocessBatch import preprocessing
#from preprocess import preprocessing
from utilities import one_hot_decoding
import os
import random
import datetime

from keras.models import load_model
global model
print('model loading...')
model = load_model("twse_cnn_model.hdf5")
print('loading completed')


class fetchbroker(object):

    def __init__(self, code, csvpath):
        self.code=code
        self.csvpath=csvpath
        
    def fetch(self):
        retry = 5
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}
        
        for _retry in range(retry):
            
            ss = requests.Session()
            resp = ss.get("https://bsr.twse.com.tw/bshtm/bsMenu.aspx", headers=headers)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            img_tags = soup.select("#Panel_bshtm img")
            src = img_tags[0].get('src')
            url_captcha="https://bsr.twse.com.tw/bshtm/" + src
            resp = ss.get(url_captcha, stream=True, headers=headers)
            
                  
            allowedChars = 'ACDEFGHJKLNPQRTUVXYZ2346789';
            CAPTCHA_IMG = "captcha.jpg"
            PROCESSED_IMG = "preprocessing.jpg"
                  
                  
            if resp.status_code == 200:
                with open(CAPTCHA_IMG, 'wb') as f:
                    resp.raw.decode_content = True
                    shutil.copyfileobj(resp.raw, f)
      

            preprocessing(CAPTCHA_IMG, PROCESSED_IMG)
            train_data = np.stack([np.array(cv2.imread(PROCESSED_IMG))/255.0])
            prediction = model.predict(train_data)

            predict_captcha = one_hot_decoding(prediction, allowedChars)

            payload = {}
            acceptable_input = ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION', 'RadioButton_Normal',
                                'TextBox_Stkno', 'CaptchaControl1', 'btnOK']

            inputs = soup.select("input")
            
            for elem in inputs:
                if elem.get("name") in acceptable_input:
                    if elem.get("value") != None:
                        payload[elem.get("name")] = elem.get("value")
                    else:
                        payload[elem.get("name")] = ""

            payload['TextBox_Stkno'] = self.code
            payload['CaptchaControl1'] = predict_captcha
        
            #csvpath='data/'    
            if not os.path.exists(self.csvpath):
                os.mkdir(self.csvpath)
     
            csvheaders={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Host': 'bsr.twse.com.tw',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'
                }
            
            resp = ss.post("https://bsr.twse.com.tw/bshtm/bsMenu.aspx", data=payload, headers=headers)
       
            if '驗證碼錯誤!' in resp.text:
                print('驗證碼錯誤, predict_captcha: ' + predict_captcha)
                print("retry", _retry)
                
            elif '驗證碼已逾期!' in resp.text:
                print('驗證碼已逾期, predict_captcha: ' + predict_captcha)
                print("retry", _retry)
            
            elif '查無資料' in resp.text:
                print('{} 查無資料'.format(self.code))
                with open(self.csvpath+'/'+'log.txt', 'a', encoding='utf8') as f:
                    f.write('{} 查無資料\n'.format(self.code))
                break
            
            elif 'HyperLink_DownloadCSV' in resp.text:
                resp=ss.get("https://bsr.twse.com.tw/bshtm/bsContent.aspx", headers=csvheaders)

                filename=self.csvpath+'/'+self.code+'_'+str(datetime.date.today())+'.csv'
                with open(filename, 'w', encoding='utf8') as f:
                    f.write(resp.text)

                print('{} is output'.format(filename))
                break