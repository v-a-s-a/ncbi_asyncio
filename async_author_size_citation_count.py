#!/usr/bin/env python
"""
Short program to plot citation counts versus author list size.
Docs for NCBI esearch:
https://www.ncbi.nlm.nih.gov/books/NBK25499/#chapter4.ESearch
"""

import itertools
from Bio import Entrez
import timeit
import math
import sys

import asyncio
import aiohttp


pub_years = ['2014']
test_pmids = ['25056061', '28622505']

class Publication:

    def __init__(self, pmid, citation_count=None, title=None, authors=None, journal=None):
        self.pmid = pmid
        self.citation_count = citation_count
        self.title = title.replace(' ', '_')
        if authors:
            self.author_count = len(authors)
        else:
            self.author_count = None
        self.journal = journal.replace(' ', '_')

        self.header = '\t'.join(["pmid", "title", "journal", "count", "author_count"])


    @classmethod
    def from_icite_record(cls, rec):
        pub = cls(pmid=rec['pmid'],
            citation_count=rec['citation_count'],
            title=rec['title'],
            authors=rec['authors'],
            journal=rec['journal'])
        return pub

    def __repr__(self):
        string = str("{pmid}\t{title}\t{journal}\t{count}\t{author_count}".format(pmid=self.pmid,
            count=self.citation_count,
            title=self.title,
            author_count=self.author_count,
            journal=self.journal))
        return string

def date_range_pub_count(years=pub_years):
    counts = 0
    for year in years:
        Entrez.email = "vassily@broadinstitute.org"
        count_handle = Entrez.esearch(db="pubmed",
            sort="relevance",
            retmode="xml",
            rettype="count",
            mindate=year,
            maxdate=year,
            field="DP",
            term=year)
        count_results = Entrez.read(count_handle)
        counts += int(count_results["Count"])
    return counts

async def get_pmid_block(chunksize=50, years=pub_years):
    """
    https://marcobonzanini.com/2015/01/12/searching-pubmed-with-python/
    Yield PMIDs in chunks of a given size.
    """
    for year in years:
        Entrez.email = "vassily@broadinstitute.org"
        count_handle = Entrez.esearch(db="pubmed",
            sort="relevance",
            retmode="xml",
            rettype="count",
            mindate=year,
            maxdate=year,
            datetype="pdat",
            field="DP",
            term=year)
        count_results = Entrez.read(count_handle)
        count = int(count_results["Count"])

        retmax_requests = list(range(chunksize, count, chunksize))
        retmax_requests.append(count - retmax_requests[len(retmax_requests) - 1])

        for i, retmax in enumerate(retmax_requests):
            pmid_handle = Entrez.esearch(db="pubmed",
                sort="relevance",
                retmode="xml",
                usehistory='y',
                retstart=retmax,
                retmax=chunksize,
                field="DP",
                term=year,
                mindate=year,
                maxdate=year,
                datetype="pdat")
            results = Entrez.read(pmid_handle)
            await results["IdList"]


async def parse_publications(pmids, out_conn):
    """
    https://icite.od.nih.gov/api
    """
    async with aiohttp.ClientSession() as session:
       async with session.get("https://icite.od.nih.gov/api/pubs", params={"pmids": ','.join(pmids)}) as icite_handle:
            icite_resp = await icite_handle.json()
            icite_data = icite_resp.get("data")

    Entrez.email = "vassily@broadinstitute.org"
    efetch_handle = Entrez.efetch(db='pubmed', id=pmids, retmode='xml')
    efetch_results = Entrez.read(efetch_handle)

    for efetch, icite in zip(efetch_results['PubmedArticle'], icite_data):
        # parse efetch and icite records
        pub = Publication.from_icite_record(icite)

        if efetch['MedlineCitation'].get('InvestigatorList'):
            pub.author_count = len(efetch['MedlineCitation']['InvestigatorList'])
        elif efetch['MedlineCitation'].get('Article').get('AuthorList'):
            pub.author_count = len(efetch['MedlineCitation']['Article']['AuthorList'])
        else:
            print("{} record is structured differently".format(pub.pmid))
        out_conn.write(str(pub) + '\n')

    # publications = [Publication.from_icite_record(rec) for rec in icite_handle.json().get("data")]

async def main(loop):

    # make this async
    pmid_blocks = chunked_pmids(chunksize=500)

    # # make this async
    # publications = (parse_publications(pmids) for pmids in pmid_blocks)
    
    # # make this async

    # # with open('publications_citation_counts.txt', 'w') as out_conn:
    # #     out_conn.write(publications[0].header + '\n')
    # #     for pub in publications:
    # #         out_conn.write(str(pub) + '\n')

    # with open('publications_citation_counts.txt', 'w') as out_conn:
    #     # start_time = timeit.default_timer()
    #     for i, block in enumerate(pmid_blocks):
    #         publications = [parse_publications(block, out_conn)]
    #         # elapsed_time = timeit.default_timer() - start_time
    #         sys.stdout.write("\r{0:.3f}% complete".format(((i+1)/(total_pubs/500))*100))
    #         sys.stdout.flush()
    #         loop.run_until_complete(asyncio.wait(publications))



def __main__():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))


if __name__ == "__main__":
    __main__()
