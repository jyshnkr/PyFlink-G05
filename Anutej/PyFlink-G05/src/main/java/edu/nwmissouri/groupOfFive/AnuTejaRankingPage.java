package edu.nwmissouri.groupOfFive;

import java.io.Serializable;
import java.util.ArrayList;

public class AnuTejaRankingPage implements Serializable{

    String name = "unknown.md";
    Double rank = 1.000;

    ArrayList<AnuTejaVotingPage> voters = new ArrayList<AnuTejaVotingPage>();

    AnuTejaRankingPage(String nameIn, ArrayList<AnuTejaVotingPage> votersIn){
        this.name = nameIn;
        this.voters = votersIn;
    }

    AnuTejaRankingPage(String nameIn, Double rankIn, ArrayList<AnuTejaVotingPage> votersIn){
        this.name = nameIn;
        this.voters = votersIn;
        this.rank = rankIn;
    }

    public Double getRank(){
        return rank;
    }
    
    public ArrayList<AnuTejaVotingPage> getVoters() {
        return voters;
    }
    
    @Override
    public String toString() {
        return String.format("%s,%.5f,%s", this.name,this.rank,this.voters.toString());
    }
    
}