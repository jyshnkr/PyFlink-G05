package edu.nwmissouri.groupOfFive;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPage implements Serializable{

    String name = "unknown.md";
    Double rank = 1.000;

    ArrayList<VotingPage> voters = new ArrayList<VotingPage>();

    RankedPage(String nameIn, ArrayList<VotingPage> votersIn){
        this.name = nameIn;
        this.voters = votersIn;
    }

    RankedPage(String nameIn, Double rankIn, ArrayList<VotingPage> votersIn){
        this.name = nameIn;
        this.voters = votersIn;
        this.rank = rankIn;
    }

    public Double getRank(){
        return rank;
    }
    
    public ArrayList<VotingPage> getVoters() {
        return voters;
    }
    
    @Override
    public String toString() {
        return String.format("%s,%.5f,%s", this.name,this.rank,this.voters.toString());
    }
    
}
