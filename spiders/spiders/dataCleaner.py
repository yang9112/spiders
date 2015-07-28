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
        return re.sub(r'<script.*?/script>', '', content)
    
    def delul(self, content):
        return re.sub(r'<ul.*?</ul>', '', content)
    
    def delnote(self, content):
        return re.sub(r'<!--.*?-->', '', content)
    
    def delstyle(self, content):
        return re.sub(r'<style.*?/style>', '', content)
    
    def delhyperlink1(self, content):
        content = re.sub(r'<a .*?</a>', '', content)
        content = re.sub(r'<a>.*?</a>', '', content)
        return content
    
    def cleanlabel(self, content):
        for label in ['</font>', '</p>', '</span>']:
            content = content.replace(label, '')

        for label in ['<font.*?>', '<p.*?>', '<strong.*?>', 
                      '<span.*?>', '<br.*?>', '<img.*?>']:            
            content = re.sub(label, '', content)
        return content
    
    def extracttxt(self, content):
        maxcontent = ''
        divmatch = re.compile('<div.*?</div>')
        doubledivmatch = re.compile('<div.*?<div')
        clearmatch = re.compile('<.*?>')
        
        if len(divmatch.findall(content)) >= 0:
            for minicontent in re.findall('>(.*?)<', content):
                minicontent = clearmatch.sub('', minicontent)
                if len(minicontent) > len(maxcontent):
                    maxcontent = minicontent
        
        while len(divmatch.findall(content)) > 0:
            for minicontent in divmatch.findall(content):
                while len(doubledivmatch.findall(minicontent)) > 0:
                    minicontent = doubledivmatch.sub('<div', minicontent)
    
                content = content.replace(minicontent, '')
                minicontent = clearmatch.sub('', minicontent)
                if len(minicontent) > len(maxcontent):
                    maxcontent = minicontent
        
        return maxcontent
            
    def process(self, content):
        if content:        
            f = content.lower()
            f = re.sub('\n|\r|\t', '', f)
            
            f = self.delscript(f)
            f = self.delul(f)
            
            f = self.delnote(f)
            f = self.delstyle(f)
            
            f = self.delhyperlink1(f)
            f = self.cleanlabel(f)
            f = self.extracttxt(f)
            return f
        else:
            return None
    
    def rep(self, content):
        for sig in self.sigrep:
           content = content.replace(sig,' ')
        return content