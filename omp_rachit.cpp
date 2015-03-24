#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>
#include <string.h>
#include <string>
#include <map>
#include <cstdlib>
#include <dirent.h>
#include <omp.h>

#define NO_READER_THREADS 2
#define NO_MAPPER_THREADS 2
#define NO_REDUCER_THREADS 2
#define NO_WRITER_THREADS 2

using namespace std;

typedef struct 
{
	int freq;
	string word;
} key_value;


int ascii_sum (string s)
{
	int sum =0;
	for (int i = 0; i<s.length(); i++)
	{
		sum=sum+s[i];
	}
	return sum;
}
int main()
{

	string s,t;
	int i,done_reading =0,done_mapping=0,done_reducing=0;
	DIR *dir;
	struct dirent *ent;
	std::vector<string> filelist;
	ofstream fout("result.txt",ofstream::app);
	if (fout==NULL) 
	{
		cout<<"Error opening File! I am exiting!"<<endl;
	}
	if ((dir = opendir ("/cygdrive/c/Users/Admin/Dropbox/Purdue/ece563/project/mapreduce/files")) != NULL)
	{
  		/* print all the files and directories within directory */
		while ((ent = readdir (dir)) != NULL) 
		{

			if(!((strcmp(ent->d_name,".")==0) || (strcmp(ent->d_name,"..")==0)))
			filelist.push_back(ent->d_name);

       		}
       		closedir (dir);
    	}
    	else
    	{
		/* could not open directory */
	 	perror ("");
	 	return EXIT_FAILURE;
	}
	//printf("at line %d\n",__LINE__);	
	vector <string> Work_Q;
	map <string, int> writer_Q;
	vector <key_value> Reducer_Q [NO_REDUCER_THREADS];
	omp_set_num_threads(NO_READER_THREADS+NO_MAPPER_THREADS+NO_REDUCER_THREADS+NO_WRITER_THREADS);	
	#pragma omp parallel
	{
		int pid = omp_get_thread_num();
		double time_elapsed=-omp_get_wtime();
		time_elapsed = -omp_get_wtime();
		if (pid<NO_READER_THREADS)  // 0,1 are reader thread
		{
			int count=0;
			string l;
			vector <string> chunk;
			ifstream ifs;
			while(!(filelist.empty()))
			{
				#pragma omp critical
				{
					if(!(filelist.empty()))
					{
						//cout << ("./files/"+filelist.back()).c_str() << endl;
						ifs.open(("./files/"+filelist.back()).c_str());
						//cout << filelist.back().c_str() <<"       "<<pid<<endl;
						filelist.pop_back();
					}
				}
				while(ifs>>l)
				{
					//printf("Are you there yet pid %d??\n",pid);
					chunk.clear();
					chunk.push_back(l);
					count=1;
					//printf("***********%s  %d   %d\n",l.c_str(),count,pid);
					while((ifs>>l && count<7+2*pid))
					{
						chunk.push_back(l);
						count++;
						//printf("***********%s  %d   %d\n",l.c_str(),count,pid);
					}
					#pragma omp critical 
					{
						Work_Q.insert(Work_Q.end(), chunk.begin(), chunk.end());
					}
				}
				if(!ifs.bad())
				{
					ifs.close();
				}
				else
				{
					printf("errorrrr\n");
				}
			}
			#pragma omp critical
			{
				done_reading++;
			}
		}
		if (pid>=NO_READER_THREADS && pid<NO_READER_THREADS+NO_MAPPER_THREADS+NO_REDUCER_THREADS) //mapper & reducer 2,3,4,5
		{
			if (pid < NO_READER_THREADS+NO_MAPPER_THREADS) //2,3
			{
				map <string, int> tuples;
				string popped;
				int valid = 0;
				while(!(Work_Q.empty() && done_reading==NO_READER_THREADS))
				{
					valid=0;
					while(Work_Q.empty()){ 
						if(done_reading==NO_READER_THREADS) break;
					}
					#pragma omp critical
					{
						if (!Work_Q.empty())
						{
							popped = Work_Q.back();   // Read the last entry in the queue and store it locally
							Work_Q.pop_back();  // Pop the last entry from the queue.
			//				printf("Popped %s, size of work_Q reduced to %d, by thread %d\n",popped.c_str(),Work_Q.size(),pid);
							valid=1;
						}
					}
					//printf("@@@@@@@@@@@@@@@%s  %d\n",popped.c_str(),Work_Q.size());
					if (valid)
					{
						if (tuples.find(popped) == tuples.end())
						{
							tuples.insert(pair<string,int> (popped,1));  // the popped value is not in map so insert it.
						}
						else
						{
							tuples[popped]++;       // popped value found in the directory! bingo! increment the count value
						}
					}
					if (tuples.size()==3+2*pid || (done_reading==NO_READER_THREADS && Work_Q.empty() && tuples.size()>0))
					{
						vector <key_value> reducer_chunk [NO_REDUCER_THREADS];
						for (map<string,int>::iterator it=tuples.begin(); it!=tuples.end(); ++it)
    						{
							key_value pair;
							pair.word = it->first;
							pair.freq = it->second;
							//printf("################## %s  %d  %d  %d\n",pair.word.c_str(),pair.freq,pid,ascii_sum(it->first)%NO_REDUCER_THREADS);
							reducer_chunk[ascii_sum(it->first)%NO_REDUCER_THREADS].push_back(pair);
						}
						#pragma omp critical
						{
							for (i=0; i< NO_REDUCER_THREADS; i++)
							{
								Reducer_Q[i].insert(Reducer_Q[i].end(),reducer_chunk[i].begin(),reducer_chunk[i].end());
					//			printf("push to %d by thread %d\n",i,pid);							
							}
						}
						for (i=0; i< NO_REDUCER_THREADS; i++)
						{
							reducer_chunk[i].clear();
						}
						tuples.clear();
					}
			//		printf("\nQ size is %d with pid %d\n",Work_Q.size(),pid);
				}
				//printf("\n");
				/*
				if(pid ==2)  //debug
				{
					for (i=0; i<NO_REDUCER_THREADS; i++)
					{
						for (int ll=0; ll<Reducer_Q[i].size(); ll++)
						printf("-----------------------------%s->%d->%d\n",(Reducer_Q[i][ll].word).c_str(),Reducer_Q[i][ll].freq,i);
					}
				}*/
				#pragma omp critical
				{
					done_mapping++;
				}
			}
			else //reducer 4,5
			{	
				map <string, int> r_tuples;
				while(!((done_mapping==NO_READER_THREADS) && (Reducer_Q[pid-NO_READER_THREADS-NO_MAPPER_THREADS].empty())))
				{
					while (!Reducer_Q[pid-NO_READER_THREADS-NO_MAPPER_THREADS].empty())
					{
//printf("size of Reducer_Q[%d] = %d by thread %d and done_reading = %d\n",pid-NO_READER_THREADS-NO_MAPPER_THREADS,Reducer_Q[pid-NO_READER_THREADS-NO_MAPPER_THREADS].size(),pid,done_reading);
						key_value pair;
						int updated =0;
						#pragma omp critical
						{
							if (!Reducer_Q[pid-NO_READER_THREADS-NO_MAPPER_THREADS].empty())
							{
								pair = Reducer_Q[pid-NO_READER_THREADS-NO_MAPPER_THREADS].back();
								Reducer_Q[pid-NO_READER_THREADS-NO_MAPPER_THREADS].pop_back();
				//				printf(">>>>>>>>>>>>>>>>>>>>>>>>>>Popped ('%s',%d) from Q %d by thread %d\n",pair.word.c_str(),pair.freq,pid-4,pid);

								updated=1;
							}
						}
						//printf("word = %s\n",pair.word.c_str());
						if (updated) // This makes sure that garbage is not added to the map
						{
							if (r_tuples.find(pair.word) == r_tuples.end())
							{
								r_tuples.insert(std::pair<string, int> (pair.word,pair.freq));  // the popped value is not in map so insert it.
							}
							else
							{
								r_tuples[pair.word]++;       // popped value found in the directory! bingo! increment the count value
							}
						}
					//	printf("\n------------------------------%s ----> %d === %d\n",(pair.word).c_str(),r_tuples[pair.word],pid);
					}
				}
				#pragma omp critical
				{
					writer_Q.insert(r_tuples.begin(),r_tuples.end());
		//			for (map<string,int>::iterator it=r_tuples.begin(); it!=r_tuples.end(); ++it)
		//				cout<<(it->first).c_str()<<"\t-----\t"<<(it->second)<<"    "<<pid<<endl;
					done_reducing++;
				}
			}
		}
		else // Writer 7,8
		{
			key_value pair;
			while (!(writer_Q.empty() && done_reducing==NO_REDUCER_THREADS))
			{
				#pragma omp critical
				{
					if (!writer_Q.empty())
					{
	       					pair.word= (writer_Q.begin())->first;
						pair.freq= (writer_Q.begin())->second;
						writer_Q.erase(writer_Q.begin());
						//cout<<(pair.word)<<"\t"<<pair.freq<<endl;
						fout<<(pair.word)<<"\t"<<pair.freq<<endl<<endl;
					}
				}
			}
		printf("Time Elapsed %f seconds\n",time_elapsed += omp_get_wtime());
		}
	}
	
	return 0;
}
