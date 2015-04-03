#ifndef K3_RUNTIME_DCDHANDLE_HPP
#define K3_RUNTIME_DCDHANDLE_HPP

#include <fstream>
#include <iostream>
#include <IOHandle.hpp>
#include <Codec.hpp>

namespace K3 {

class DCDIStreamHandle : public IOHandle {

  public :
    DCDIStreamHandle ( string path ) 
      : IOHandle(shared_ptr<FrameCodec>()), LogMT("DCDIStreamHandle"), currentFrame(0) 
    {
	try{
                dcdFile.open(path, std::ios::in|std::ios::binary);
		readHeader();
		
        }
        catch (std::ifstream::failure e){
                BOOST_LOG(*this) << "Can't read the file !";
        }


    }	
    bool isInput() { return true; }

    bool isOutput() { return false; }

    bool hasRead() { 
    	return currentFrame < NFILE && dcdFile.good() ;
    }

    shared_ptr<string> doRead() {
      currentFrame++;
      return readOneFrame();
    }

    bool hasWrite() {
      BOOST_LOG(*this) << "Invalid write operation on input handle";
      return false;
    }

    void doWrite(shared_ptr<string> data) {
      BOOST_LOG(*this) << "Invalid write operation on input handle";
    }

    void close() {
	dcdFile.close();
    }

    bool builtin() { return false; }
    bool file() { return true; }

    IOHandle::SourceDetails networkSource()
    {
      return make_tuple(shared_ptr<FrameCodec>(), shared_ptr<Net::NEndpoint>());
    }

    IOHandle::SinkDetails networkSink()
    {
      return make_tuple(shared_ptr<FrameCodec>(), shared_ptr<Net::NConnection>());
    }
  

  private :
	std::fstream dcdFile ;

	char HDR[4+1];
	int ICNTRL[20];

	int  NTITLE; //how many "title lines" in dcd
	char *TITLE; //each "title line" is 80 char long.

	int NFILE;  //ICNTRL(1)  number of frames in this dcd
	int NPRIV;  //ICNTRL(2)  if restart, total number of frames before first print
	int NSAVC;  //ICNTRL(3)  frequency of writting dcd
	int NSTEP;  //ICNTRL(4)  number of steps ; note that NSTEP/NSAVC = NFILE
	int NDEGF;  //ICNTRL(8)  number of degrees of freedom
	int FROZAT; //ICNTRL(9) is NATOM - NumberOfFreeAtoms : it is the number of frozen (i.e. not moving atoms)
	int DELTA4; //ICNTRL(10) timestep in AKMA units but stored as a 32 bits integer !!!
	int QCRYS;  //ICNTRL(11) is 1 if CRYSTAL used.
	int CHARMV; //ICNTRL(20) is charmm version

	int NATOM; // Number of atoms
	int LNFREAT; // Number of free (moving) atoms.
	
	int currentFrame ;
	double pbc[6];

	shared_ptr<string> readOneFrame(){

		unsigned fortCheck1, fortCheck2;
		int siz = LNFREAT;
		shared_ptr<string> result = make_shared<string>();
		result->resize(3*sizeof(float)*siz + sizeof(int));
		char* p = const_cast<char*>(result->data());
		memcpy(p, &siz, sizeof(int));
		p += sizeof(int);
		 
		if (QCRYS){
			dcdFile.read((char*)&fortCheck1, sizeof(unsigned));
			dcdFile.read((char*)&pbc, sizeof(double)*6);
			dcdFile.read((char*)&fortCheck2, sizeof(unsigned));

		}
		
		dcdFile.read((char*)&fortCheck1, sizeof(unsigned));
		dcdFile.read( p, sizeof(float)*siz);
		dcdFile.read((char*)&fortCheck2, sizeof(unsigned));
		
		dcdFile.read((char*)&fortCheck1, sizeof(unsigned));
		dcdFile.read( p +  sizeof(float)*siz, sizeof(float)*siz);
		dcdFile.read((char*)&fortCheck2, sizeof(unsigned));

		dcdFile.read((char*)&fortCheck1, sizeof(unsigned));
		dcdFile.read(p+2*sizeof(float)*siz, sizeof(float)*siz);
		dcdFile.read((char*)&fortCheck2, sizeof(unsigned));

		return result;
	}

	void readHeader(){

		unsigned fortCheck1, fortCheck2;

		//Consistency check 1
		dcdFile.read((char*)&fortCheck1, sizeof(unsigned));

		//First data block written by fortran : a character array of length 4
		dcdFile.read((char*)HDR, sizeof(char)*4 );

		//Second data block written by fortran : an integer(4) of length 20	
		dcdFile.read((char*)ICNTRL, sizeof(int)*20 );

		//Consistency check 2
		dcdFile.read((char*)&fortCheck2, sizeof(unsigned));


		HDR[4]='\0';
		NFILE = ICNTRL[0];
		cout << "Number of frames : "<< NFILE << endl;
		
		NPRIV = ICNTRL[1];



		NSAVC = ICNTRL[2];
		NSTEP = ICNTRL[3];
		NDEGF = ICNTRL[7];
		FROZAT= ICNTRL[8];
		DELTA4= ICNTRL[9];
		QCRYS = ICNTRL[10];
		CHARMV= ICNTRL[19];

		for (unsigned i = 0; i< 20; ++i){
			cout<<ICNTRL[i]<<"\t";
		}
		cout << endl;

		// Reading the number of titles
		dcdFile.read((char*)&fortCheck1, sizeof(unsigned));
		dcdFile.read((char*)&NTITLE, sizeof(int));
		if (NTITLE == 0 ){
			TITLE = new char[80+1];
			TITLE[0]='\0';
		}

		else {
			TITLE = new char[NTITLE*80+1];
			for (unsigned it=0; it< NTITLE; it++){
				dcdFile.read((char*)&TITLE[it*80], sizeof(char)*80);

			}
			TITLE[NTITLE*80]='\0';
		}

		dcdFile.read((char*)&fortCheck2, sizeof(unsigned));

		cout << "n Title : "<< NTITLE<<endl;
		// Reading the number of atoms 
		dcdFile.read((char*)&fortCheck1, sizeof(unsigned));
		dcdFile.read((char*)&NATOM, sizeof(int));
		dcdFile.read((char*)&fortCheck2, sizeof(unsigned));


		LNFREAT = NATOM - FROZAT;
		cout << "Number of atoms : "<< NATOM << endl;
		cout << "Number of  free atoms : "<< LNFREAT << endl;

		pbc[0] = pbc[1]= pbc[2] = pbc[3] = pbc[4] = pbc[5] = 0.0;
	}
};

} // end namespace K3

#endif 

