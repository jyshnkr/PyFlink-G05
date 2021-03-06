package edu.nwmissouri.groupOfFive;

import java.io.Serializable;
import java.util.ArrayList;

public class ThotaRankedPage implements Serializable{

    String name = "unknown.md";
    Double rank = 1.000;
    ArrayList<ThotaVotingPage> voters = new ArrayList<ThotaVotingPage>();

    ThotaRankedPage(String nameIn, ArrayList<ThotaVotingPage> votersIn) {
        this.name = nameIn;
        this.voters = votersIn;
    }
    ThotaRankedPage(String nameIn,Double rankIn, ArrayList<ThotaVotingPage> votersIn){
        this.name = nameIn;
        this.voters = votersIn;
        this.rank = rankIn;

    }

    public Double getRank() {
        return rank;
    }



    public ArrayList<ThotaVotingPage> getVoters() {
        return voters;
    }

    @Override
    public String toString() {
      
        return String.format("%s,%.5f,%s", this.name,this.rank,this.voters.toString());
    }


    
}
