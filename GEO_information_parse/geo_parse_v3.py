# -*- coding: utf-8 -*-  
#from csv import list_dialects
#from pickle import NONE, TRUE
#from sre_parse import _OpSubpatternType
import pandas as pd
import os, sys
#import shutil
import xml.dom.minidom
from xml.dom.minidom import parse
from collections import Counter
import requests
import re
import subprocess
import datetime
import tarfile
pd.set_option("display.max_columns", None)

def un_tar(dirpath, file):
	obs_path = os.path.join(dirpath, file)
	tar = tarfile.open(obs_path)
	untar_files = tar.getnames()
	for untar_name in untar_files:
		if untar_name.endswith('_family.xml'):
			tar.extract(untar_name, dirpath)
			break
	tar.close()
	return untar_name

def parse_try(func):
	def wrapper(*args, **kwargs):
		try:
			func_result = func(*args, **kwargs)
		except Exception as einfo:
			print(f'---error: {einfo}')
			func_result = None
			func_status = False
		else:
			func_status = True
		return func_status, func_result
	return wrapper

def get_contributor_infor(contributor, geo_id): #contributor infor
	contributor_dic={}
	contributor_dic["iid"]=contributor.attributes["iid"].value
	for tag in ["Email","Phone","Fax","Laboratory","Department"]:
		contributor_dic[tag]=""
		tag_list=contributor.getElementsByTagName(tag)
		for t in tag_list:
			for c in t.childNodes:
				contributor_dic[tag]+=c.nodeValue
				#print(f'---cat_contricutor1: {c.nodeValue}')
	for tag in ["Person","Address"]:
		tag_list=contributor.getElementsByTagName(tag)
		for t in tag_list:
			contributor_dic[tag]=""
			for c in t.childNodes:
				for n in c.childNodes:
					contributor_dic[tag]+=c.tagName+":"+n.wholeText+"; "
			#print(f'---cat_contributor2: {contributor_dic[tag]}')
	return pd.DataFrame.from_dict(contributor_dic,orient="index").T

def deal_contributor_infor(contributors_result, series_result, geo_id):
	contributors_result['Name'] = contributors_result['Person'].apply(get_dataset_author_name)
	contrs = list_remove(list(contributors_result['Name']))
	storage_dataset.loc[geo_id, 'contributors'] = ' | '.join(contrs)
	contact_id = series_result['Contact'].values[0]
	#print(f'{contact_id}')
	#print(f'{contributors_result}')
	storage_dataset.loc[geo_id, 'contact'] = contributors_result[contributors_result['iid'] == contact_id]['Email'].values[0]
# 	contributors_dic = {}
# 	contact_dic = {}
# 	for acc in acc_list:
# 		contrs = list(Authors[Authors['acc']==acc]['Name'])
# 		contrs = list_remove(contrs)
# 		contributors_dict = ' | '.join(contrs)
# 		contact_id = GEO_Infor.loc[GEO_Infor['iid'] == acc]['Contact']
# 		contact_dic[acc]=Authors[(Authors['acc']==acc)&(Authors['iid']==contact_id.values[0])]['Email'].values[0]
# 	Platform_Infor['title'] = Platform_Infor["Title"].apply(math_platform_title)
def deal_platforms_infor(platform_result, geo_id):
	ids = list_remove(list(platform_result['iid']))
	titles = list_remove(list(platform_result['Title']))
	platforms = [re.match('([^(]*).*',title).group(1).strip() for title in titles]
	storage_dataset.loc[geo_id, 'platform_id'] = ' | '.join(ids)
	storage_dataset.loc[geo_id, 'platforms'] = ' | '.join(platforms)
	return platform_result
	# for acc in list(Platform_Infor['acc'].unique()):
	# 	id=list(Platform_Infor[Platform_Infor['acc']==acc]['iid'])
	# 	if '' in id:
	# 		id.remove('')
	# 	platformids_dic[acc] = ' | '.join(id)
	# 	platforms=[]
	# 	titles=list(Platform_Infor[Platform_Infor['acc']==acc]['Title'])
	# 	for title in titles:
	# 		tmp=re.match('([^(]*).*',title)
	# 		title=tmp.group(1).strip()
	# 		platforms.append(title)
	# 	platforms_dic[acc] = ' | '.join(platforms)

def deal_samples_infor(sample_result, platform_result, geo_id):
	title_dict = platform_result.set_index('Accession')['Title'].to_dict()
	sample_result['platforms'] = sample_result['Platform-Ref'].map(title_dict)
	return sample_result
def deal_series_infor(series_result, geo_id):
	series_result['Accession'] = series_result['iid'].apply(lambda x: f'[{"{"}"name":"GEO Series Accessions","value":"{x}","url":"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={x}"{"}"}]')
	return series_result
def get_platforms_infor(Platform, geo_id): #platform infor
	Platform_dic={}
	Platform_dic["iid"]=Platform.attributes["iid"].value
	for tag in ["Title","Accession","Technology","Organism","Distribution",
				"Manufacturer","Manufacture-Protocol","External-Data"]:
		tag_list=Platform.getElementsByTagName(tag)
		for t in tag_list:
			for c in t.childNodes:
				Platform_dic[tag]=c.nodeValue.strip().strip("\n")
	for tag in["Web-Link"]:
		tag_list=Platform.getElementsByTagName(tag)
		Platform_dic[tag]=""
		for t in tag_list:
			for c in t.childNodes:
				Platform_dic[tag]+=c.wholeText+"; "
	for tag in["Description"]:
		tag_list=Platform.getElementsByTagName(tag)
		Platform_dic[tag]=""
		for t in tag_list[:1]:
			for c in t.childNodes:
				Platform_dic[tag]+=c.wholeText.strip().strip("\n")
	for tag in ["Status"]:
		tag_list=Platform.getElementsByTagName(tag)
		for t in tag_list:
			Platform_dic[tag]=""
			for c in t.childNodes:
				for n in c.childNodes:
					Platform_dic[tag]+=c.tagName+":"+n.wholeText+"; "
	#print(f'---cat_platform: {Platform_dic}')
	platform = pd.DataFrame.from_dict(Platform_dic,orient="index").T
	return platform
def get_Characteristics_info(char,Sample_dic):
	subchar=char.attributes["tag"].value
	if subchar == 'tissue' or subchar  == 'organ' or subchar  == 'mouse organ':
		subchar ='tissues'
	if subchar  == 'major cell type':
		subchar ='cell_types'
	if subchar  == 'region':
		subchar ='tissue region'
	if subchar  == 'disease state':
		subchar ='disease'
	if subchar  == 'mouse strain':
		subchar ='strain'
	if subchar  == 'development stage' or subchar  == 'embryonic stage' or subchar  == 'Stage':
		subchar ='development_stage'
	if subchar  == 'age' or subchar =='developmental stage':
		subchar ='development_stage'
	if subchar  == 'Sex' or subchar == 'gender':
		subchar ='sex'
	slelect_char=['tissues','tissue region','disease','cell line','strain','genotype','development_stage','cell_types','sex']
	if subchar in slelect_char:
		if subchar in Sample_dic:
			Sample_dic[subchar]+=' | '+char.childNodes[0].nodeValue.strip().strip("\n")
		else:
			Sample_dic[subchar]=char.childNodes[0].nodeValue.strip().strip("\n")
	return Sample_dic

def get_dataset_author_name(x):
	first=""
	middle=""
	last=""
	if x == '':
		return ""
	names=x.split("; ")
	for name in names:
		if name.startswith("First:"):
			first=name.split(":")[1].strip(";")
		if name.startswith("Last:"):
			last=name.split(":")[1].strip(";")
		if name.startswith("Middle:"):
			middle=name.split(":")[1].strip(";")       
	if middle!="":
		return last+", "+first+" "+middle
	else:
		return last+", "+first

def get_samples_infor(Sample, geo_id): #sample infor
	Sample_dic={}
	Sample_dic["iid"]=Sample.attributes["iid"].value
	Status=Sample.getElementsByTagName('Status')
	Sample_dic["submission_date"] = Status[0].getElementsByTagName('Submission-Date')[0].childNodes[0].data
	Sample_dic["last_update_date"] =Status[0].getElementsByTagName('Last-Update-Date')[0].childNodes[0].data
	for tag in ["Title","Accession","Type","Channel-Count","Hybridization-Protocol","Scan-Protocol",
				"Description","Data-Processing","Supplementary-Data","Library-Strategy","Library-Source","Library-Selection"]:
		tag_list=Sample.getElementsByTagName(tag)
		for t in tag_list:
			for c in t.childNodes:
				Sample_dic[tag]=c.nodeValue.strip().strip("\n")
	for tag in["Platform-Ref","Contact-Ref"]:
		tag_list=Sample.getElementsByTagName(tag)
		for t in tag_list:
			Sample_dic[tag]=t.attributes["ref"].value
	for tag in["Channel"]:
		Channels=Sample.getElementsByTagName(tag)
		for Channel in Channels:
			pos=Channel.attributes["position"].value
			for sub_tag in ["Source","Organism","Characteristics","Treatment-Protocol","Growth-Protocol",
					"Molecule","Extract-Protocol","Label","Label-Protocol",]:
				tag_list=Channel.getElementsByTagName(sub_tag)
				for t in tag_list:
					for c in t.childNodes:
						if t.attributes:
							if "tag" in t.attributes and sub_tag=='Characteristics':# <Characteristics
								Sample_dic = get_Characteristics_info(t,Sample_dic)
								if sub_tag in Sample_dic:
									Sample_dic[sub_tag]+=t.attributes["tag"].value+":"+c.nodeValue.strip()+"; "
								else:
									Sample_dic[sub_tag]=t.attributes["tag"].value+":"+c.nodeValue.strip()+"; "
							elif "taxid" in t.attributes and sub_tag=='Organism' :#<Organism
								if sub_tag in Sample_dic:
									#Sample_dic[sub_tag]+=t.attributes["taxid"].value+":"+c.nodeValue.strip()+"; "
									Sample_dic[sub_tag]+=' | '+ c.nodeValue.strip()
									Sample_dic['taxid']+=' | '+ t.attributes["taxid"].value
								else:
									Sample_dic[sub_tag]=c.nodeValue.strip()
									Sample_dic['taxid']=t.attributes["taxid"].value
							else:
								Sample_dic[tag+"-"+pos+":"+sub_tag]=c.nodeValue.strip().strip("\n")
						else:
							Sample_dic[tag+"-"+pos+":"+sub_tag]=c.nodeValue.strip().strip("\n")
	for tag in ["Status"]:
		tag_list=Sample.getElementsByTagName(tag)
		for t in tag_list:
			Sample_dic[tag]=""
			for c in t.childNodes:
				for n in c.childNodes:
						Sample_dic[tag]+=c.tagName+":"+n.wholeText+"; "
	for chracter in ['tissues','tissue region','disease','cell line','strain','genotype','developmental_stages','cell_types','sex']:
		if chracter not in  Sample_dic:
			Sample_dic[chracter]=""
	sample = pd.DataFrame.from_dict(Sample_dic,orient="index").T
	return sample

def get_series_infor(Series, geo_id):
	Series_dic={}
	Series_dic["iid"]=Series.attributes["iid"].value
	for tag in ["Title","Accession","Pubmed-ID","Summary","Overall-Design",
				"Description","Data-Processing","Supplementary-Data"]:
		tag_list=Series.getElementsByTagName(tag)
		for t in tag_list:
			for c in t.childNodes:
				Series_dic[tag]=c.nodeValue.strip().strip("\n")
	Series_dic["Contact"]=Series.getElementsByTagName("Contact-Ref")[0].attributes["ref"].value
	for tag in["Type"]:
		Types_list=Series.getElementsByTagName(tag)
		Series_dic[tag]=""
		for Type in Types_list:
				for c in Type.childNodes:
					Series_dic[tag]+=c.nodeValue.strip().strip("\n")+"; "
	Status=Series.getElementsByTagName('Status')
	Series_dic["submission_date"] = Status[0].getElementsByTagName('Submission-Date')[0].childNodes[0].data
	Series_dic["last_update_date"] =Status[0].getElementsByTagName('Last-Update-Date')[0].childNodes[0].data
	series = pd.DataFrame.from_dict(Series_dic,orient="index").T
	return series

def deal_platform(collection, geo_id):
	contributors=collection.getElementsByTagName("Contributor")
	R = []
	for contributor in contributors:
		contributor_infor=get_contributor_infor(contributor)
		R.append(contributor_infor)
	platforms = pd.concat(R).reset_index(drop=True).fillna("")
	platforms["acc"] = geo_id
	return platforms

def deal_function(collection, geo_id, filed, function):
	filed_contents = collection.getElementsByTagName(filed)
	R = []
	for filed_content in filed_contents:
		filed_infor = function(filed_content, geo_id)
		R.append(filed_infor)
	fileds_infor = pd.concat(R).reset_index(drop=True).fillna('')
	fileds_infor["acc"] = geo_id
	return fileds_infor

def list_remove(alist, rmlist = []):
	while '' in alist:
		alist.remove('')
	for rm in rmlist:
		while rm in alist:
			alist.remove(rm)
	return alist

def math_platform_title(title):
	tmp=re.match('([^(]*).*',title)
	title=tmp.group(1).strip()
	return title

def serpmid(pmid):
	url='https://pubmed.ncbi.nlm.nih.gov/'+pmid+'/citations/'
	hd={"user-agent":"Mozallia/5.0"}
	try:
		r=requests.request("get",url,headers=hd)
		import json
		a=json.loads(r.text)
		return a['mla']['orig']
	except:
		print(f'{pmid} Crawling failed')
		
def collect_xml(ilist, item):
	#('dir', 'C:\\Users\\maxizheng\\Desktop\\2023_11\\GSE_parse\\'); ('list', 'xxx.list')
	xml_names = []
	if ilist == 'None':
		for file in os.listdir(item):
			if file.endswith("_family.xml.tgz"):
				xml_names.append(file.rstrip('_family.xml.tgz'))
			elif file.endswith("_family.xml"):
				xml_names.append(file.rstrip('_family.xml'))
			else:
				pass
	else:
		list_file = open(ilist, 'rt')
		xml_names = [file.strip().rstrip('_family.xml') for file in list_file.readlines()]
	#else:
		#exit(f"collect xml file type wrong, type: {itype}")
	xml_names = list(set(xml_names))
	return xml_names

def deal_xml(xml_name):
	xml_file_rel = xml_name+'_family.xml'
	if not os.path.exists(os.path.join(idir, xml_file_rel)):
		xml_tgz = xml_name+'_family.xml.tgz'
		xml_file_rel = un_tar(idir, xml_tgz)
	xml_file = os.path.join(idir, xml_file_rel)
	return xml_file, xml_file_rel

		
if __name__ == "__main__":
	GEO_Infor=[]; series_result = []
	Authors=[]; contributors_result = []
	Platform_Infor=[]; platform_result = []
	Sample_Infor=[]; sample_result = []
	xml_count = 0
	today_date=datetime.datetime.now().strftime('%Y-%m-%d')

	ilist = 'None'
	idir = 'C:\\Users\\maxizheng\\Desktop\\2023_11\\GSE_parse\\'
	
	xml_names = collect_xml(ilist, idir)
	print(f'---check: xml files total num {len(xml_names)}')

	storage_dataset = pd.DataFrame()
	storage_sample = pd.DataFrame()
	for xml_name in xml_names:
		xml_count += 1
		xml_file, xml_file_rel = deal_xml(xml_name)
		geo_id = xml_file_rel.split('_')[0]
		print(f'---run: {geo_id}, {xml_file_rel}, {xml_count}')

		DOMTree = xml.dom.minidom.parse(os.path.join(idir, xml_file_rel))
		collection = DOMTree.documentElement

		deal_func_dict = {
					"Contributor" : {"function" : get_contributor_infor, "result" : "contributors_result", "list" : "Authors"},
					"Platform" : {"function" : get_platforms_infor, "result" : "platform_result", "list" : "Platform_Infor"},
					"Sample" : {"function" : get_samples_infor, "result" : "sample_result", "list" : "Sample_Infor"},
					"Series" : {"function" : get_series_infor, "result" : "series_result", "list" : "GEO_Infor"}
					}
		# #contributors_result = deal_function(collection, geo_id, "Contributor", deal_func_dict["Contributor"]["function"])
		# for tag in deal_func_dict.keys():
		# 	globals()[deal_func_dict[tag]["result"]]= deal_function(collection, geo_id, tag, deal_func_dict[tag]["function"])
		# 	#print(f'---check result:\n{deal_func_dict}\n----aa{contributors_result}')
		# 	globals()[deal_func_dict[tag]["list"]].append(eval(deal_func_dict[tag]["result"]))
		contributors_result = deal_function(collection, geo_id, "Contributor", deal_func_dict["Contributor"]["function"])
		platform_result = deal_function(collection, geo_id, "Platform", deal_func_dict["Platform"]["function"])
		sample_result = deal_function(collection, geo_id, "Sample", deal_func_dict["Sample"]["function"])
		series_result = deal_function(collection, geo_id, "Series", deal_func_dict["Series"]["function"])
		deal_contributor_infor(contributors_result, series_result, geo_id)
		deal_platforms_infor(platform_result, geo_id)
		deal_samples_infor(sample_result, platform_result, geo_id)
		deal_series_infor(series_result, geo_id)
		for tag in deal_func_dict.keys():
			globals()[deal_func_dict[tag]["list"]].append(eval(deal_func_dict[tag]["result"]))
	Authors = pd.concat(Authors).reset_index(drop=True).fillna("")
	Platform_Infor = pd.concat(Platform_Infor).reset_index(drop=True).fillna("")
	Sample_Infor = pd.concat(Sample_Infor).reset_index(drop=True).fillna("")
	GEO_Infor = pd.concat(GEO_Infor).reset_index(drop=True).fillna("")
	print(f'---check: {GEO_Infor}')
	print('parse part work done!')

	GEO_Infor = GEO_Infor.set_index('iid', drop=False)
	dataset_result = pd.concat([GEO_Infor, storage_dataset], axis = 1)
	dataset_result = dataset_result.rename(columns={"iid":"dataset_id",
						"Title":'title',
						"Summary":"summary",
						"Overall-Design":"overall_design",
						"Accession":"accessions",
						"Pubmed-ID":"pmid",
						"last_update_date":"last_modified",
						"Contact" : "contacts",
						"last_update_date" : "last_modified",
						})
	dataset_result["citation"]=dataset_result["pmid"].apply(serpmid)
	dataset_columns = ['dataset_id', 'title', 'species', 'tissues', 'organ_parts', 'cell_types', 'cells', 'spots', 'genes', 'development_stages', 'sex', 'stomics_technologies', 'sample_number', 'section_number', 'disease', 'browser', 'summary', 'overall_design', 'submission_date', 'last_modified', 'contributors', 'contacts', 'citation', 'accessions', 'platforms', 'dataset_quality', 'pmid', 'status', 'reference']
	
	Sample_Infor = Sample_Infor.set_index('iid', drop=False)
	sample_result = pd.concat([Sample_Infor, storage_sample], axis = 1)
	sample_result=sample_result.rename(columns={"iid" : 'sample_id',
							"Title":'sample name',
							"last_update_date":"last_modified",
							'Organism':"species",'tissues':"tissue",
							'Channel-1:Source':'sample_title'
						   })
	sample_result['source']='GEO'
	sample_columns = ['dataset_id', 'sample_id', 'sample name', 'sample_title', 'source', 'species', 'tissue', 'organ_parts', 'development_stage', 'disease', 'sex',
     'technology', 'platforms', 'cell_types', 'sample_quality', 'explore', 'visualization', 'download', 'tissue region', 'taxid', 'iid', 'Characteristics', 'submission_date',
	 'last_modified', 'Accession', 'Type', 'Channel-Count', 'Description', 'Data-Processing', 'Supplementary-Data', 'Library-Strategy', 'Library-Source', 'Library-Selection', 'Platform-Ref',
	 'Contact-Ref', 'Channel-1:Molecule', 'Channel-1:Extract-Protocol', 'Status', 'cell line', 'strain', 'genotype', 'acc', 'Channel-1:Treatment-Protocol','Channel-1:Growth-Protocol']
	
	dataset_result = dataset_result.reindex(columns=dataset_columns)
	sample_result=sample_result.reindex(columns=sample_columns)
	dataset_result.to_excel(idir+'/GEO_Dataset_Infor_'+today_date+'.xlsx',sheet_name='datasets',index=False)
	sample_result.to_excel(idir+'/GEO_Sample_Infor_'+today_date+'.xlsx',sheet_name='samples',index=False)
	dataset_result.to_csv(idir+'/GEO_Dataset_Infor_'+today_date+'.tsv', sep='\t', index = False)
	sample_result.to_csv(idir+'/GEO_Sample_Infor_'+today_date+'.tsv', sep='\t',index=False)
	print(f'status: Write result done!')