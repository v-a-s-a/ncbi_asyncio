#!/usr/bin/env python

import asyncio
import aiohttp
import xmltodict
import pickle
import requests as req

from aiohttp.helpers import DummyCookieJar

class Publication:
    '''
    "pmid": 23456789,
    "authors": "Arun Sharma, Stephen M Schwartz, Eduardo Méndez",
    "citation_count": 11,
    "citations_per_year": 4.665397,
    "expected_citations_per_year": 3.022486,
    "field_citation_rate": 5.335023,
    "is_research_article": true,
    "journal": "Cancer",
    "nih_percentile": 66.200000,
    "relative_citation_ratio": 1.543563,
    "title": "Hospital volume is associated ...",
    "year": 2013
    '''

    def __init__(self, pmid, citation_count=None, title=None, authors=None, journal=None,
                    relative_citation_ratio=None, citations_per_year=None,
                    expected_citations_per_year=None, field_citation_rate=None,
                    is_research_article=None, year=None):
        self.pmid = pmid
        self.citation_count = citation_count

        self.citations_per_year = citations_per_year
        self.expected_citations_per_year = expected_citations_per_year
        self.field_citation_rate = field_citation_rate
        self.is_research_article = is_research_article
        self.year = year
        
        if title:
            self.title = title.replace(' ', '_')
        else:
            self.title = title

        if journal:
            self.journal = journal.replace(' ', '_')
        else:
            self.journal = journal

        if authors:
            self.author_count = len(authors)
        else:
            self.author_count = None

        if relative_citation_ratio:
            self.relative_citation_ratio = relative_citation_ratio
        else:
            self.relative_citation_ratio = None

        

    @classmethod
    def from_icite_record(cls, rec):
        pub = cls(pmid=rec['pmid'],
            citation_count=rec['citation_count'],
            title=rec['title'],
            authors=rec['authors'],
            journal=rec['journal'],
            relative_citation_ratio=rec['relative_citation_ratio'],
            citations_per_year=rec['citations_per_year'],
            expected_citations_per_year=rec['citations_per_year'],
            field_citation_rate=rec['field_citation_rate'],
            is_research_article=rec['is_research_article'],
            year=rec['year'])
        return pub

    def __repr__(self):
        fields = (self.pmid, self.citation_count, self.title, self.author_count,
                            self.journal, self.relative_citation_ratio, self.citations_per_year,
                            self.expected_citations_per_year, self.field_citation_rate,
                            self.is_research_article, self.year)
        string = '\t'.join((str(x) for x in fields))
        return string

    
    def print_header(self):
        header= '\t'.join(('pmid', 'citation_count', 'title', 'author_count',
                            'journal', 'relative_citation_ratio', 'citations_per_year',
                            'expected_citations_per_year', 'field_citation_rate',
                            'is_research_article', 'year'))
        return header


async def get_pmid_count(session):
    base_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?'

    req_params = {'db': 'pubmed', 
                  'retmode': 'json',
                  'rettype': 'uilist',
                  'usehistory': 'y',
                  'mindate': '2014',
                  'maxdate': '2014',
                  'datetype': 'pdate',
                  'field': 'DP',
                  'term': '2014'}
    async with session.get(base_url, params=req_params) as resp:
        assert resp.status == 200
        resp_data = await resp.json()

    count = int(resp_data['esearchresult']['count'])
    webenv = resp_data['esearchresult']['webenv']
    query_key = resp_data['esearchresult']['querykey']

    return (count, webenv, query_key)


async def get_pmid_block(session, total_pmids, webenv, query_key, block_size=100):
    '''
    https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=PNAS[ta]+AND+97[vi]&retstart=6&retmax=6&tool=biomed3
    '''
    base_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?'

    retmax_requests = list(range(block_size, int(total_pmids), block_size))
    retmax_requests.append(total_pmids - retmax_requests[len(retmax_requests) - 1])

    for i, retmax in enumerate(retmax_requests):
        percent_done = (float(i) / float(len(retmax_requests))) * 100

        req_params = params={'db': 'pubmed',
                            'retmode': 'json',
                            'email': 'vassily@broadinstitute.org',
                            'usehistory': 'y',
                            'retstart': retmax,
                            'retmax': block_size,
                            'webenv': webenv,
                            'query_key': query_key,
                            'field': 'DP',
                            'term': '2014',
                            'mindate': '2014',
                            'maxdate': '2014',
                            'datetype': 'pdat'}

        # async version also times out for some reason -- worth posting about
        async with session.get(base_url, params=req_params) as resp:
            assert resp.status == 200
            resp_data = await resp.json()
            webenv = resp_data['esearchresult']['webenv']
            query_key = resp_data['esearchresult']['querykey']
            yield (resp_data['esearchresult']['idlist'], percent_done)

def reg_get_pmid_block(session, total_pmids, webenv, query_key, block_size=500):
    '''
    https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=PNAS[ta]+AND+97[vi]&retstart=6&retmax=6&tool=biomed3
    '''
    base_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?'

    retmax_requests = list(range(block_size, int(total_pmids), block_size))
    retmax_requests.append(total_pmids - retmax_requests[len(retmax_requests) - 1])

    for i, retmax in enumerate(retmax_requests):
        percent_done = (float(i) / float(len(retmax_requests))) * 100

        req_params = params={'db': 'pubmed',
                            'retmode': 'json',
                            'email': 'vassily@broadinstitute.org',
                            'usehistory': 'y',
                            'retstart': retmax,
                            'retmax': block_size,
                            'webenv': webenv,
                            'query_key': query_key,
                            'field': 'DP',
                            'term': '2014',
                            'mindate': '2014',
                            'maxdate': '2014',
                            'datetype': 'pdat'}

        try:
            resp = req.get(base_url, params=req_params)
            assert resp.status_code == 200
            pmid_block = resp.json()
            id_list = pmid_block['esearchresult']['idlist']
        except:
            id_list = []
        yield (id_list, percent_done, i)     

async def get_author_count(session, pmids):
    base_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?'
    async with session.get(base_url,
                params={'db': 'pubmed', 'id': ','.join(pmids), 'retmode': 'xml'}) as efetch_handle:
        assert efetch_handle.status == 200

        try:
            efetch_data = await efetch_handle.text()
            data = xmltodict.parse(efetch_data, xml_attribs=True)
        except:
            print(efetch_handle)
            data = {}

        return data


async def get_icite(session, pmids):
    async with session.get('https://icite.od.nih.gov/api/pubs',
                params={'pmids': ','.join(pmids)}) as icite_handle:
        assert icite_handle.status == 200

        try:
            icite_resp = await icite_handle.json()
        except:
            print(icite_resp)
            icite_resp = {'data': {}}
        return icite_resp.get('data')


def reg_get_icite(pmids):
    icite_handle = req.get('https://icite.od.nih.gov/api/pubs', params={'pmids': ','.join(pmids)})
    assert icite_handle.status == 200

    try:
        icite_resp = icite_handle.json()
    except:
        print(icite_resp)
        icite_resp = {'data': {}}

    return icite_resp.get('data')

async def main(loop):
    async with aiohttp.ClientSession(loop=loop, cookie_jar=aiohttp.DummyCookieJar()) as session:

        pmid_count, webenv, query_key = await get_pmid_count(session)
        print(pmid_count)

        for pmid_block, percent_done, block_num in reg_get_pmid_block(session, pmid_count, webenv, query_key):
            print(percent_done)
            icite_recs = reg_get_icite(pmid_block)
            efetch_recs = reg_get_author_count(pmid_block)

            with open('data/pubmed_esearch/block_{}.pickle'.format(block_num), 'wb') as out_conn:
                pickle.dump(efetch_recs, out_conn)
            
            with open('data/icite/block_{}.pickle'.format(block_num), 'wb') as out_conn:
                pickle.dump(icite_recs, out_conn)


def __main__():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))


if __name__ == '__main__':
    __main__()
