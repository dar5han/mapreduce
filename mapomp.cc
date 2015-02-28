#include<iostream>
#include<fstream>
#include<string>
#include<sstream>
#include<omp.h>
#include<stdlib.h>
#include<dirent.h>
#include<vector>
#include<string.h>

using namespace std;


int main()
{
	DIR *dir;
	struct dirent *ent;
	std::vector<string> filelist;

	if ((dir = opendir ("/home/min/a/dramesh/openmp/project/test/")) != NULL)
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
	

	#pragma omp parallel shared(filelist) num_threads(2)
	{
		
			while(!(filelist.empty()))
			{
			#pragma omp critical
			{
				if(!(filelist.empty()))
				{
				int n = omp_get_num_threads();
				string str="./test/"+filelist.back();
				printf("thread id %d\n",omp_get_thread_num());	
				string l;
				filelist.pop_back();
			        	
				cout << str.c_str()<<"\n";

				ifstream file;
				file.open(str.c_str());

				if(!file.bad())
				{
					while(file >> l)
		        			cout << l << endl; 
					file.close();
				}
				else
				{
					printf("errorrrr\n");
				}
			   } 
			}
		}
	}
	printf("done\n");

        	return 0;
}
