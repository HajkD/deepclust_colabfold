from argparse import ArgumentParser
import subprocess
import cluster_index
class DCD_MMSeqs:
    def __init__(self,path_to_mmseqs, path_to_DCD, path_to_DCD_index, path_to_output,path_to_tmp, threads, search_param, filter_param, db_load_mode):
        self.path_to_output = path_to_output
        self.threads = threads
        self.search_param = search_param
        self.filter_param = filter_param
        self.path_to_mmseqs = path_to_mmseqs
        self.path_to_DCD = path_to_DCD
        self.path_to_DCD_index = path_to_DCD_index
        self.path_to_tmp = path_to_tmp
        self.db_load_mode = db_load_mode

    def createMMseqsDB(self, fasta_file: str, output_file : str):
        command = self.path_to_mmseqs + ' createdb' + ' ' + fasta_file + ' ' + output_file + ' --shuffle 0'
        print("Executing: ", command)
        subprocess.check_call([command],shell=True)

    def concatDB(self, db_one: str, first: bool, db_two: str = ''):
        if first:
            command = self.path_to_mmseqs + ' concatdbs' + ' ' + self.path_to_output + db_one + ' ' + self.path_to_output + db_two + ' ' + self.path_to_output + 'DCD_full'
            command2 = self.path_to_mmseqs + ' concatdbs' + ' ' + self.path_to_output + db_one + '_h ' + self.path_to_output + db_two + '_h ' + self.path_to_output + 'DCD_full_h'
        else:
            command = self.path_to_mmseqs + ' concatdbs' + ' ' + self.path_to_output + ' DCD_full' + ' ' + self.path_to_output + db_one + ' ' + self.path_to_output + 'DCD_full'
            command2 = self.path_to_mmseqs + ' concatdbs' + ' ' + self.path_to_output + 'DCD_full_h ' + self.path_to_output + db_two + '_h ' + self.path_to_output + 'DCD_full_h'

        print("Executing: ", command)
        subprocess.check_call([command],shell=True)
        print("Executing: ", command2)
        subprocess.check_call([command2],shell=True)


    def removeDB(self, db_path: str):
        command = self.path_to_mmseqs + ' rmdb' + ' ' + self.path_to_output + db_path
        print("Executing: ", command)
        subprocess.check_call([command],shell=True)

    def MMseqsSearch(self, path_to_query,path_to_reference,path_to_result,path_to_tmp):
        #mmseqs search prof_res metagenomic_db res_env tmp3 --threads strthreads + search_param
        command = self.path_to_mmseqs + ' search ' + path_to_query + ' ' + path_to_reference + ' ' + path_to_result +' ' + path_to_tmp + ' --threads ' + str(self.threads)+ ' ' + self.search_param
        print("Executing ", command)
        subprocess.check_call([command],shell=True)

    def MMseqsAlign(self, path_to_query, path_to_reference, path_to_prev_result, path_to_result, align_evalue,max_accept):
        command = self.path_to_mmseqs + ' align ' + path_to_query + ' ' + path_to_reference + ' ' + path_to_prev_result + ' ' + path_to_result + ' --db-load-mode ' + str(self.db_load_mode) + ' -e ' + str(align_evalue) + ' --max-accept ' + str(max_accept) + ' --threads ' + str(self.threads) + ' --alt-ali 10 -a'
        print("Executing: ", command)
        subprocess.check_call([command],shell=True)
    def MMseqsConvertAlis(self, path_to_query, path_to_reference, path_to_res, path_to_output):
        command = self.path_to_mmseqs + ' convertalis ' + ' ' + path_to_query + ' ' + path_to_reference + ' ' + path_to_res + ' ' + path_to_output +  ' --threads ' + str(self.threads) + ' --format-output query,target'
        print("Executing: ", command)
        subprocess.check_call([command],shell=True)

    def MMseqsFilterResults(self, path_to_query, path_to_reference, path_to_align_res, path_to_output, qsc):
        command = self.path_to_mmseqs + ' filterresult ' + ' ' + path_to_query + ' ' + path_to_reference + ' ' + path_to_align_res + ' ' + path_to_output + ' --db-load-mode ' + str(self.db_load_mode) + ' --qid 0 --qsc ' + str(qsc) + ' --diff 0 --max-seq-id 1.0 --threads ' + str(self.threads) +  ' --filter-min-enable 100'
        print("Executing: ", command)
        subprocess.check_call([command],shell=True)

    def MMseqsResultToMSA(self, path_to_query, path_to_reference, path_to_filter_res, path_to_output):
        command = self.path_to_mmseqs + ' result2msa ' + path_to_query + ' ' + path_to_reference + ' ' + path_to_filter_res + ' ' + path_to_output + ' --msa-format-mode 6 --db-load-mode ' + str(self.db_load_mode) + ' --threads ' + str(self.threads) + ' ' + self.filter_param
        print("Executing: ", command)
        subprocess.check_call([command],shell=True)

    def extractClusterAndWriteToMMseqs(self,path_to_centroids_extract, per_clust_output):
        clust_ext = cluster_index.search_for_cluster(path_to_DCD= self.path_to_DCD, path_to_index=self.path_to_DCD_index,path_to_centroids=path_to_centroids_extract, path_to_output=self.path_to_output, per_clust_output= per_clust_output, max_num_of_cluster_at_once = 0,threads = self.threads, verbose = False)
        clust_ext.dataRetrievalParallel(path_to_centroids_extract, from_mmseqs=True)
        print("Extracting Cluster from DCD")
        self.createMMseqsDB(self.path_to_output + 'DCD_all_members.fa', output_file = self.path_to_output + 'DCD_mmseqs')

    def MMseqsUnpackDatabase(self,path_to_result, path_to_output):
        command = self.path_to_mmseqs + ' unpackdb '+ path_to_result + ' ' + path_to_output + ' --unpack-name-mode 0 --unpack-suffix .a3m'
        print("Executing: ", command)
        subprocess.check_call([command], shell=True)

    def MMseqsLinkDatabase(self, path_to_first, path_to_output):
        command = self.path_to_mmseqs + ' lndb ' + path_to_first + ' ' + path_to_output
        print("Executing: ", command)
        subprocess.check_call([command], shell=True)

    def PipelineDCDMMseqs(self, path_to_query,path_to_uniref, path_to_DCD_centroids,align_evalue,max_accept,qsc):

        print("To DO")
        '''TO DO'''
        '''
        1. Query against Uniref -> uni_profile (search)
        '''
        if(path_to_uniref):
            self.MMseqsSearch(path_to_query= path_to_query, path_to_reference=path_to_uniref,path_to_result= self.path_to_tmp + 'uniref_res', path_to_tmp= self.path_to_tmp + 'query_uniref')
            path_to_profile = self.path_to_tmp + "/query_uniref/latest/profile_1"
            self.MMseqsLinkDatabase(path_to_query + '_h',self.path_to_tmp + "query_uniref/latest/profile_1_h")
        else:
            path_to_profile = path_to_query
        '''
        2. uni_profile against Centroids from DCD -> cent_profile (search)
        '''
        self.MMseqsSearch(path_to_query= path_to_profile, path_to_reference=path_to_DCD_centroids,path_to_result= self.path_to_tmp + 'uni_prof_cent_res', path_to_tmp= self.path_to_tmp + 'uniref_cent')
        print("First search complete")
        '''
        3. extract clusters based on cent_profile -> cent_db (convertalis)
        '''
        self.MMseqsConvertAlis(path_to_query,path_to_DCD_centroids,self.path_to_tmp + 'uni_prof_cent_res',self.path_to_tmp + 'cent_hit.ls')
        self.extractClusterAndWriteToMMseqs(path_to_centroids_extract=self.path_to_tmp + 'cent_hit.ls', per_clust_output=False)
        '''
        4. cent_profile against cent_db -> double_cent_res (search)
        '''
        path_to_query_2 = self.path_to_tmp + 'uniref_cent/latest/profile_1'
        self.MMseqsSearch(path_to_query= path_to_query_2, path_to_reference=self.path_to_output + 'DCD_mmseqs',path_to_result= self.path_to_tmp + 'double_cent_res', path_to_tmp= self.path_to_tmp + 'tmp_double_cent_res')
        '''
        (4.5 Realign based on double_cent_res ?! (align))
        '''
        path_to_query_3 = self.path_to_tmp + 'tmp_double_cent_res/latest/profile_1'
        self.MMseqsAlign(path_to_query=path_to_query_3,path_to_reference=self.path_to_output + 'DCD_mmseqs',path_to_prev_result=self.path_to_tmp + 'double_cent_res',path_to_result=self.path_to_tmp + 'double_cent_res_realign', align_evalue=align_evalue, max_accept=max_accept)
        '''
        5. filter result in double_cent_res -> final_align (filterresult)
        '''
        self.MMseqsFilterResults(path_to_query=path_to_query, path_to_reference=self.path_to_output + 'DCD_mmseqs', path_to_align_res=self.path_to_tmp + 'double_cent_res_realign',path_to_output = self.path_to_tmp + 'double_cent_res_realign_filter', qsc=qsc)
        '''
        6. result2msa
        '''
        self.MMseqsResultToMSA(path_to_query= path_to_query, path_to_reference=self.path_to_output + 'DCD_mmseqs',path_to_filter_res=self.path_to_tmp + 'double_cent_res_realign_filter', path_to_output='uniref30_dcd_msa.a3m')
        '''
        Unpack results to per query MSA
        '''
        self.MMseqsUnpackDatabase(path_to_result="uniref30_dcd_msa.a3m", path_to_output=self.path_to_output)
def main():
    parser = ArgumentParser()
    parser.add_argument("-path_to_query", type=str,
                        help="Path to query (Profile or sequence)", )
    parser.add_argument("-path_to_mmseqs", type=str,
                        help="Path to mmseqs", )
    parser.add_argument("-path_to_DCD", type=str,
                        help="Location of the DeepClust database in parquet format", )
    parser.add_argument("-path_to_centroids", type = str,
                         help="Path to DCD centroids in mmseqs format")
    parser.add_argument("--path_to_uniref", type=str,
                        help="Path to uniref in mmseqs format",default=None)
    parser.add_argument("-path_to_DCD_index", type=str,
                        help="Location of the DeepClust index", )
    parser.add_argument("-path_to_output", type=str,
                        help="Path to output", )
    parser.add_argument("-path_to_tmp", type=str,
                        help="Path to temporary directory", )
    parser.add_argument("--threads", type=int, default=1,
                        help="Threads")
    print("Documentation available at https://github.com/drostlab/deepclust_colabfold")
    print("Please cite: https://www.biorxiv.org/content/10.1101/2023.01.24.525373v1 bioRxiv (2023)")
    args = parser.parse_args()
    dcd_mm = DCD_MMSeqs(path_to_mmseqs = args.path_to_mmseqs, path_to_DCD = args.path_to_DCD,
                        path_to_DCD_index = args.path_to_DCD_index,
                        path_to_output = args.path_to_output,
                        path_to_tmp = args.path_to_tmp,
                        threads = args.threads,
                        search_param = "--num-iterations 3 --db-load-mode 2 -a -e 0.1 --max-seqs 10000 -s 3.1",
                        filter_param = "--filter-msa 1 --filter-min-enable 1000 --diff 3000 --qid 0.0,0.2,0.4,0.6,0.8,1.0 --qsc 0 --max-seq-id 0.95",
                        db_load_mode = 2)
    dcd_mm.PipelineDCDMMseqs(args.path_to_query,path_to_uniref=args.path_to_uniref, path_to_DCD_centroids=args.path_to_centroids,align_evalue = 10,max_accept=100000,qsc =0.8)




if __name__ == '__main__':
    main()
