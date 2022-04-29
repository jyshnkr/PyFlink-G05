package edu.nwmissouri.groupOfFive.nandinikandi;

import java.io.Serializable;

public class VotingPage implements Serializable{

    public String nameIn = "'unknown.md";
    public Double rank = 1.0;
    public Integer votes = 0;

    public VotingPage(String nameIn, Integer contributorVotes) {
        this.nameIn = nameIn;
        this.votes = contributorVotes;
    }

    public VotingPage(String contributingPageName, Double contributingPageRank, Integer votes) {
        this.nameIn = contributingPageName;
        this.rank = contributingPageRank;
        this.votes = votes;
    }

    public String getNameIn(){
        return this.nameIn;
    }

    public void setNameIn(String nameIn){
        this.nameIn = nameIn;
    }

    public  Double getRank(){
        return this.rank;
    }
    
    public void setRank(Double rank ){
        this.rank = rank;
    }

    public  Integer getVotes(){
        return this.votes;
    }

    public void setVotes(Integer votes ){
        this.votes = votes;
    }

    @Override
    public String toString(){
        return String.format("%s, %.5f, %d", this.nameIn, this.rank, this.votes);
    }

}
