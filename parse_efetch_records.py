
import pickle
import os

from async_pubmed_pmid_blocks import Publication



def __main__():
    icite_dir, dirs, icite_pickles = next(os.walk('data/icite/'))

    pick = icite_pickles[0]
    icite_rec = pickle.load(open(icite_dir + pick, 'rb'))
    pub = Publication.from_icite_record(icite_rec)
    header = pub.print_header()

    with open('2014_complete_icite_records.tsv', 'w') as out_conn:
        out_conn.write(header + '\n')
        out_conn.write(str(pub) + '\n')

        import pdb
        pdb.set_trace()

        for pick in icite_pickles:
            pmid = pick.replace('.pickle', '')

            icite_rec = pickle.load(open(icite_dir + pick, 'rb'))

            pub = Publication.from_icite_record(icite_rec)

            try:
                efetch_rec = pickle.load(open('data/pubmed/' + pick, 'rb'))
                if efetch_rec['MedlineCitation'].get('InvestigatorList'):
                    pub.author_count = len(efetch_rec['MedlineCitation']['InvestigatorList'])
                elif efetch['MedlineCitation'].get('Article').get('AuthorList'):
                    pub.author_count = len(efetch['MedlineCitation']['Article']['AuthorList'])
            except:
                continue

            out_conn.write(str(pub) + '\n')



if __name__ == '__main__':
    __main__()