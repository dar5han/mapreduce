#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>
#include <string>
#include <cstdlib>
#include <omp.h>

using namespace std;

int main()

{

	string s,t;
	int i;
	vector <string> Work_Q;
	omp_set_num_threads(2);	
	#pragma omp parallel shared(Work_Q)
	{
		int count=0;
		int pid = omp_get_thread_num();
		double time_elapsed;
		string l;
		vector <string> chunk;
		ifstream ifs;
		if (pid == 0)
		{
			ifs.open("s.txt");
			time_elapsed = -omp_get_wtime();
		}
		else
		{
			ifs.open("s.txt");
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
		if (pid == 0) 	printf("Time Elapsed = %2f\n",time_elapsed+omp_get_wtime());	
	}
/*	for(i=0; i<Work_Q.size(); i++)
        {
             	cout<<Work_Q[i]<<endl;
        }
*/	return 0;
}
	
	
	
	
	


		
