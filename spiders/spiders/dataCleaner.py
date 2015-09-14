#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')

class dataCleaner():
    sigrep = set(['&quot;', '&oelig;', '&tilde;', '&zwj;', '&lsquo;',
                 '&bdquo;', '&rsaquo;', '&amp;', '&Scaron;', '&ensp;',
                 '&lrm;', '&rsquo;', '&dagger;', '&euro;', '&lt;',
                 '&scaron;', '&emsp;', '&rlm;', '&sbquo;', '&Dagger;',
                 '&gt;', '&Yuml;', '&thinsp;', '&ndash;', '&ldquo;',
                 '&permil;', '&OElig;', '&circ;', '&zwnj;', '&mdash;',
                 '&rdquo;','&lsaquo;','&nbsp;','&middot;','&times;'
                 ])
                 
    newlabels = set()
    
    def __init__(self, flabel='./label.txt', fsigrep='./sig.txt'):
        pass

    def delscript(self, content):
        return re.sub(r'<script.*?</script>', '', content)
        
    def deltextarea(self, content):
        return re.sub(r'<textarea.*?</textarea>', '', content)
    
    def delul(self, content):
        content = re.sub(r'<ul .*?</ul>', '', content)
        return re.sub(r'<ul>.*?</ul>', '', content)
        
    def defli(self, content):
        content = re.sub(r'<li .*?</li>', '',content)
        return re.sub(r'<li>.*?</li>', '',content)
    
    def delnote(self, content):
        return re.sub(r'<!--.*?-->', '', content)
    
    def delstyle(self, content):
        content = re.sub(r'<style .*?</style>', '', content)
        return re.sub(r'<style>.*?</style>', '', content)
    
    def deloption(self, content):
        content = re.sub(r'<option .*?</option>', '', content)
        return re.sub(r'<option>.*?</option>', '', content)

#    #do not need to clear the hyperlink in content    
#    def delhyperlink1(self, content):
#        content = re.sub(r'<a .*?</a>', '', content)
#        content = re.sub(r'<a>.*?</a>', '', content)
#        return content
    def wfromreplace(self, content):
        labels = ['<span>' , '<strong>', '<p>', '<font>',
                  '</span>', '</strong>', '</p>', '</font>']        
        for label in labels:
            content = content.replace(label, '&lleft;' + label[1:-1] + '&lright;')

        labels = ['<p .*?>', '<font .*?>', '<img .*?>', 
                  '<span .*?>', '<br.*?>', '<strong .*?>']
        for labeltp in labels:
            for label in re.findall(labeltp, content):
                content = content.replace(label, '&lleft;' + label[1:-1] + '&lright;')        
        return content
        
    def wfromtback(self, content):
        return content.replace('&lleft;', '<').replace('&lright;', '>')
        
    def cleanlabel(self, content):
        for label in ['</a>']:
            content = content.replace(label, '&aright;')

        for label in ['<a .*?>']:            
            content = re.sub(label, '&aleft;', content)
        return content
    
    def extracttxt(self, content):
        maxcontent = ''
        divmatch = re.compile('<div.*?</div>')
        doubledivmatch = re.compile('<div.*?<div')
        clearmatch = re.compile('<.*?>')
        clearmatch2 = re.compile('<.*?>|&aleft;.*?&aright;')
        
        content = self.wfromreplace(content)

        if len(divmatch.findall(content)) >= 0:
            for minicontent in re.findall('>(.*?)<', content):
                minicontent = clearmatch.sub('', minicontent)
                minicontent = self.wfromtback(minicontent)
                if len(clearmatch2.sub('', minicontent)) > len(maxcontent):
                    maxcontent = minicontent
        
        while len(divmatch.findall(content)) > 0:
            for minicontent in divmatch.findall(content):
                while len(doubledivmatch.findall(minicontent)) > 0:
                    minicontent = doubledivmatch.sub('<div', minicontent)
    
                content = content.replace(minicontent, '')
                minicontent = clearmatch.sub('', minicontent)
                minicontent = self.wfromtback(minicontent)
                if len(clearmatch2.sub('', minicontent).replace(' ','')) > len(maxcontent):
                    maxcontent = minicontent
        
        maxcontent = maxcontent.replace('&aleft;', '').replace('&aright;', '')
        return maxcontent
            
    def process(self, content):
        if content:        
            f = content.lower()
            for label in ['\n', '\r', '\t']:
                f = f.replace(label, '')
            
            f = self.delscript(f)
            f = self.deltextarea(f)
            f = self.delul(f)
            f = self.defli(f)
            
            f = self.delnote(f)
            f = self.delstyle(f)
            f = self.deloption(f)
            
            #f = self.delhyperlink1(f)
            f = self.cleanlabel(f)
            f = self.extracttxt(f)
            f = self.rep(f)
            return f
        else:
            return None
    
    def rep(self, content):
        for sig in self.sigrep:
           content = content.replace(sig,' ')
        return content
        