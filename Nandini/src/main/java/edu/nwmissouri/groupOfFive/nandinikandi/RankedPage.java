package edu.nwmissouri.groupOfFive.nandinikandi;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPage implements Serializable{

    public String nameIn = "unknown.md";
    public Double rank = 1.000;
    public ArrayList<VotingPage> voters = new ArrayList<VotingPage>();


    public RankedPage(String nameIn, ArrayList<VotingPage> voters) {
        this.nameIn = nameIn;
        this.voters = voters;
    }

    public RankedPage(String contributingPageName, Double contributingPageRank, ArrayList<VotingPage> voters) {
        this.nameIn = contributingPageName;
        this.rank = contributingPageRank;
        this.voters = voters;
    }

    public String getNameIn(){
        return nameIn;
    }

    public  ArrayList<VotingPage> getVoters(){
        return voters;
    }

    public Double getRank(){
        return rank;
    }

    public void setRank(Double rank ){
        this.rank = rank;
    }

    public void setNameIn(String nameIn){
        this.nameIn = nameIn;
    }

    public  void setVoters(ArrayList<VotingPage> voters){
        this.voters = voters;
    }

    @Override
    public String toString(){
        return String.format("%s, %.5s, %s", this.nameIn, this.rank, this.voters.toString());
    } 

}
