#!/usr/bin/env python

import asyncio
import aiohttp
import xmltodict
import pickle
import requests as req


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


async def get_pmid_block(session, block_size=500):
    '''
    https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=PNAS[ta]+AND+97[vi]&retstart=6&retmax=6&tool=biomed3
    '''
    base_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?'

    req_params = {'db': 'pubmed', 
                  'retmode': 'json',
                  'rettype': 'count',
                  'mindate': '2014',
                  'maxdate': '2014',
                  'datetype': 'pdate',
                  'field': 'DP',
                  'term': '2014'}
    async with session.get(base_url, params=req_params) as resp:
        assert resp.status == 200
        count_data = await resp.json()

    count = int(count_data['esearchresult']['count'])

    retmax_requests = list(range(block_size, int(count), block_size))
    retmax_requests.append(count - retmax_requests[len(retmax_requests) - 1])

    for i, retmax in enumerate(retmax_requests):
        percent_done = (float(i) / float(len(retmax_requests))) * 100

        req_params = params={'db': 'pubmed',
                            'retmode': 'json',
                            'usehistory': 'y',
                            'retstart': retmax,
                            'retmax': block_size,
                            'field': 'DP',
                            'term': '2014',
                            'mindate': '2014',
                            'maxdate': '2014',
                            'datetype': 'pdat'}

        resp = req.get(base_url, params=req_params)
        assert resp.status_code == 200
        pmid_block = resp.json()
        yield (pmid_block['esearchresult']['idlist'], percent_done)

        # async version also times out for some reason -- worth posting about
        # async with session.get(base_url, params=req_params) as resp:
        #     assert resp.status == 200
        #     pmid_block = await resp.json()
        #     yield  (pmid_block['esearchresult']['idlist'], percent_done)
            

async def get_author_count(session, pmids):
    base_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?'
    async with session.get(base_url,
                params={'db': 'pubmed', 'id': ','.join(pmids), 'retmode': 'xml'}) as efetch_handle:
        assert efetch_handle.status == 200

        efetch_data = await efetch_handle.text()
        return xmltodict.parse(efetch_data, xml_attribs=True)


async def get_icite(session, pmids):
    async with session.get('https://icite.od.nih.gov/api/pubs',
                params={"pmids": ','.join(pmids)}) as icite_handle:
        assert icite_handle.status == 200
        icite_resp = await icite_handle.json()
        return icite_resp.get("data")


async def main(loop):
    async with aiohttp.ClientSession(loop=loop) as session:
        async for pmid_block, percent_done in get_pmid_block(session):
            print(percent_done)
            icite_recs = await get_icite(session, pmid_block)
            pm_block = await get_author_count(session, pmid_block)

            for article in pm_block['PubmedArticleSet']['PubmedArticle']:
                pmid = article['MedlineCitation']['PMID']['#text']
                with open('data/pubmed_esearch/{}.pickle'.format(pmid), 'wb') as out_conn:
                    pickle.dump(article, out_conn)
            
            for rec in icite_recs:
                with open('data/icite/{}.pickle'.format(pmid), 'wb') as out_conn:
                    pickle.dump(rec, out_conn)


def __main__():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))


if __name__ == '__main__':
    __main__()
