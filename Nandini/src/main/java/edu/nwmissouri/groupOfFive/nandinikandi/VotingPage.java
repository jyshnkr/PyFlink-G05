package edu.nwmissouri.groupOfFive.nandinikandi;

public class VotingPage {

    public String voterName;
    public Integer contributorVotes;

    public VotingPage(String voterName, Integer contributorVotes) {
        this.voterName = voterName;
        this.contributorVotes = contributorVotes;
    }

    public String getVoterName(){
        return voterName;
    }

    public  Integer getContributorVotes(){
        return contributorVotes;
    }

    public void setVoterName(String voterName){
        this.voterName = voterName;
    }

    public void setContributorVotes(Integer contributorVotes ){
        this.contributorVotes = contributorVotes;
    }

}


  