import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import json
import threading


def process_dist(dist):
    temp= dist.split(" ")
    if(temp[1] == "m"):
        return float(temp[0])
    else:
        return float(temp[0]) * 1000


def fetch_data(num, link):
    req = requests.get(link)
    soup = BeautifulSoup(req.text, 'html.parser')
    if req.status_code != 200:
        print("ERROR:" + link)
    item = {}
    item["link"] = link    
    
    load = json.loads("".join(soup.find("script", {"type":"application/ld+json"}).contents))
    load[0]["geo"]["latitude"]
    item["latitude"] = load[0]["geo"]["latitude"]
    item["longitude"] = load[0]["geo"]["longitude"]
    
    
    overview = soup.find("tbody", {"class": "css-twrsy5"})
    if(overview!=None):
        for row in overview:
            item[row.th.text] = row.td.text
    
    topsection = soup.find("section",{"class":"css-13dph6"}).findChildren()
    if(topsection!=None):
        for i in topsection:
            content = i.findChildren()
            if(len(content)==2 and content[1].text!=""):
                item[content[1].text] = content[0].text
            if(len(content)>0 and "floor" in content[-1].text):
                item["floorinfo1"] = content[0].text
                item["floorinfo2"] = content[-1].text
    
    locality = soup.find('section', {"id": "localityInfo"})
    if(locality!=None):
        for div in locality.div.div.div:
            cc = 0
            for c in div.children:
                cc += 1
                if cc == 1:
                    place = c.text
                else:
                    tbodies = c.findAll('tbody')
                    for tbody in tbodies:
                        title = tbody.parent.parent.parent.parent.text.split(' ')[0]
                        tds = tbody.findAll('td')
                        itr = 0
                        for td in tds:
                            if(itr==0):
                                item["Closest "+title] = td.text 
                            if(itr==1):
                                item["Closest "+ title + " distance"] = process_dist(td.text)
                                break
                            itr+=1
        
    if(soup.find("div",{"class":"css-1ru8tnn"})!=None):
        addr = soup.find("div",{"class":"css-1ru8tnn"}).text
        item["address"] = addr
    
    if("resale" in item["link"]):
        item['sale_type'] = "resale"
    else:
        item['sale_type'] = "new"
    
    typeP = soup.find("h1",{"class":"css-10rvbm3"}).text
    bhk = typeP[0]
    item['bhk'] = bhk
    if("Apartment" in typeP):
        item['property_type'] = "Apartment"
    elif("Independent House" in typeP):
        item['property_type'] = "Independent House"
    elif("Independent Floor" in typeP):
        item['property_type'] = "Independent Floor"
    elif("Plot" in typeP):
        item['property_type'] = "Plot"
    elif("Studio" in typeP):
        item['property_type'] = "Studio"
    elif("Duplex" in typeP):
        item['property_type'] = "Duplex"
    elif("Penthouse" in typeP):
        item['property_type'] = "Penthouse"
    elif("Villa" in typeP):
        item['property_type'] = "Villa"
    elif("Agricultural Land" in typeP):
        item['property_type'] = "Agricultural Land"
    
    if(soup.find("button",{"class":"cta css-10jnrqa"})!=None):
        button = soup.find("button",{"class":"cta css-10jnrqa"}).text
        if("Seller" in button):
            item["listed by"] = "agent"
        else:
            item["listed by"] = "owner"
    
    if(soup.find("div",{"class":"css-mlhpw2"})!=None):
        emi = soup.find("div",{"class":"css-mlhpw2"}).text
        item["emi"] = emi.strip("EMI starts at ₹")
    
    amen_list = []
    if(soup.find('section', {"id": "amenities"})!=None):
        amen = soup.find('section', {"id": "amenities"}).section.div
        if(amen!=None):
            for div in amen:
                amen_list.append(div.text)
    furn_list = []
    if(soup.find('section', {"id": "furnishings"})!=None):
        furn = soup.find('section', {"id": "furnishings"}).section.div
        if(furn!=None):
            for div in furn:
                furn_list.append(div.text)

    item["amen"] = amen_list
    item["furn"] = furn_list
    
    item["Avg. Price"] = item["Avg. Price"].strip("₹")
    
    #print(item)
    return item
    



def fetch_and_csv(filenumber):
    links = []
    f = open(f"files/{filenumber}.txt", "r")
    for x in f:
        if(x!='\n'):
            links.append(x)
    itr=0
    df = pd.DataFrame()
    for link in links:
        try:
            df = df.append(fetch_data(filenumber, link),ignore_index=True)
            itr+=1
            if(itr % 20 == 0):
                print(str(filenumber) + ": Done "+ str(itr) + " of "+ str(len(links)))
            if itr % 10 == 0:
                df.to_csv(f"output/{filenumber}.csv")
        except:
            print(str(filenumber) + ": Error occured at: "+ str(itr) + ", " + link)
            itr+=1
    print(f"{filenumber} done")
    df.to_csv(f"output/{filenumber}.csv",index=False)



def thread_call(start, end):
    for i in range(start, end):
        fetch_and_csv(i)
        print(i)

thr = []
n = 118
num_threads = 23
for i in range(num_threads):   
    thr.append(threading.Thread(target = thread_call, args = (i*5+3, i*5+5)))
    print(i)
thr.append(threading.Thread(target = thread_call, args = (118,119)))


for t in thr:
    t.start()