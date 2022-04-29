package edu.nwmissouri.groupOfFive;

import java.io.Serializable;
import java.util.ArrayList;

public class JayaShankarRankedPage implements Serializable{

    String name = "unknown.md";
    Double rank = 1.000;

    ArrayList<JayaShankarVotingPage> voters = new ArrayList<JayaShankarVotingPage>();

    JayaShankarRankedPage(String nameIn, ArrayList<JayaShankarVotingPage> votersIn){
        this.name = nameIn;
        this.voters = votersIn;
    }

    JayaShankarRankedPage(String nameIn, Double rankIn, ArrayList<JayaShankarVotingPage> votersIn){
        this.name = nameIn;
        this.voters = votersIn;
        this.rank = rankIn;
    }

    public Double getRank(){
        return rank;
    }
    
    public ArrayList<JayaShankarVotingPage> getVoters() {
        return voters;
    }
    
    @Override
    public String toString() {
        return String.format("%s,%.5f,%s", this.name,this.rank,this.voters.toString());
    }
    
}
