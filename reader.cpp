#include<iostream>
#include<fstream>
#include<string>
#include<omp.h>
#include<stdlib.h>

using namespace std;


int main()
{
	std::string filename="file",out=".txt";
	#pragma omp parallel num_threads(2) private(filename)
	{	
		int a = omp_get_thread_num();
		char* str = itoa(a);
		std::string num = string(str);
		
		std::string filename2 = filename+num+out;
		cout << filename2 <<"thread num"+str;
		ifstream file1(filename2);

		if( !file1.bad() )
		{
			#pragma omp critical
				cout << file1.rdbuf();
			file1.close();

		}
	}

	return 0;
}
