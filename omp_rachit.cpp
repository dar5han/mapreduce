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
	int i,done =1;
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
		while ((ent = readdir (dir)) != NULL) {
        	//printf ("%s\n", ent->d_name);
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
	printf("at line %d\n",__LINE__);	
	vector <string> Work_Q;
	map <string, int> writer_Q;
	vector <key_value> Reducer_Q [NO_REDUCER_THREADS];
	omp_set_num_threads(NO_READER_THREADS+NO_MAPPER_THREADS+NO_REDUCER_THREADS+NO_WRITER_THREADS);	
	#pragma omp parallel
	{
		int count=0;
		int pid = omp_get_thread_num();
		double time_elapsed;
		string l;
		vector <string> chunk;
		ifstream ifs;
		time_elapsed = -omp_get_wtime();
		if (pid<NO_READER_THREADS)  // 0,1 are reader thread
		{
			while(!(filelist.empty()))
			{
				printf("at line %d thread id %d\n",__LINE__,omp_get_thread_num());
				cout << ("./files/"+filelist.back()).c_str() << endl;
				#pragma omp critical
				{
					if(!(filelist.empty()))
					{
					cout << ("./files/"+filelist.back()).c_str() << endl;
					ifs.open(("./files/"+filelist.back()).c_str());
					cout << filelist.back().c_str() << endl;
					filelist.pop_back();
					}
				}

				while(ifs>>l)
				{
					chunk.clear();
					chunk.push_back(l);
					count=0;
					while(ifs>>l || count<14+2*pid)
					{
						chunk.push_back(l);
						count++;	
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
				if (pid == 0) 	printf("Time Elapsed = %2f\n",time_elapsed+omp_get_wtime());
				printf("at line %d thread id %d\n",__LINE__,omp_get_thread_num());
			}
		}
		if (pid>=NO_READER_THREADS && pid<NO_READER_THREADS+NO_MAPPER_THREADS+NO_REDUCER_THREADS) //mapper & reducer 2,3,4,5
		{
				if (pid < NO_READER_THREADS+NO_MAPPER_THREADS) //2,3
				{
					map <string, int> tuples;
					string popped;
					#pragma omp critical
					{
					popped = Work_Q.back();   // Read the last entry in the queue and store it locally
					Work_Q.pop_back();  // Pop the last entry from the queue.
				}
				if (tuples.find(popped) == tuples.end())
				{
					tuples.insert(pair<string,int> (popped,1));  // the popped value is not in map so insert it.
				}
				else
				{
					tuples[popped]++;       // popped value found in the directory! bingo! increment the count value
				}
				if (tuples.size()==10)
				{
					vector <key_value> reducer_chunk [NO_REDUCER_THREADS];
					for (map<string,int>::iterator it=tuples.begin(); it!=tuples.end(); ++it)
    					{
						key_value pair;
						pair.word = it->first;
						pair.freq = it->second;
						reducer_chunk[ascii_sum(it->first)%NO_REDUCER_THREADS].push_back(pair);
					}
					#pragma omp critical
					{
						for (i=0; i< NO_REDUCER_THREADS; i++)
						Reducer_Q[i].insert(Reducer_Q[i].end(),reducer_chunk[i].begin(),reducer_chunk[i].end());
					}
					for (i=0; i< NO_REDUCER_THREADS; i++)
					{
						reducer_chunk[i].erase(reducer_chunk[i].begin(),reducer_chunk[i].end());
					}
					tuples.erase(tuples.begin(),tuples.end());
				}
			}
			else //reducer 4,5
			{	
				map <string, int> r_tuples;
				while(!done)
				{
					if (!Reducer_Q[pid-NO_READER_THREADS-NO_MAPPER_THREADS].empty())
					{
						key_value pair;
						pair = Reducer_Q[pid-NO_READER_THREADS-NO_MAPPER_THREADS].back();
						Reducer_Q[pid-NO_READER_THREADS-NO_MAPPER_THREADS].pop_back();
						if (r_tuples.find(pair.word) == r_tuples.end())
						{
							r_tuples.insert(std::pair<string, int> (pair.word,pair.freq));  // the popped value is not in map so insert it.
						}
						else
						{
							r_tuples[pair.word]++;       // popped value found in the directory! bingo! increment the count value
						}
					}
				}
				#pragma omp critical
				{
					writer_Q.insert(r_tuples.begin(),r_tuples.end());
				}
				
			}
		}
		else // Writer 7,8
		{
			key_value pair;
			while (!writer_Q.empty())
			{
				#pragma omp critical
				{
			       		pair.word= (writer_Q.begin())->first;
					pair.freq= (writer_Q.begin())->second;
					writer_Q.erase(writer_Q.begin());
				}
				#pragma omp critical
				{
					fout<<pair.word<<"\t"<<pair.freq<<endl;
				}
			}
		}
	}
	return 0;
}
	
	
	
	
	


		
