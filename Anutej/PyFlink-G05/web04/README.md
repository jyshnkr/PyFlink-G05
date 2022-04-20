# README: 4-Page Example Web

[Go](go.md)
[Java](java.md)
[Python](python.md)

## Suggestions

- Start with the wordcount quickstart code.
- Mock up the initial 4-block test case.

1A:

- Begin by reading in each file to lines.
- Then check each line for a link.
- If a link, output: (page, outlink)

1B: 

- Group by key (page, value, list of outlinks)

2A:

- Map to reverse and add page values. Could have many records for each key. 

2B:

- For each key, use all incoming links to calc PR.
- Repeat until page ranks don't change. 

## Verify As You Work

- What is the total value of this 4-page web?
- How many links are there to process?

## Simplifications

- All links start in column 1. 
- Links have no spaces. 
- Pages have no images.
- No page points to itself.
- All pages have at least one incoming link. 
- All pages have at least one outgoing link. 

## Challenges

Improve until the links:

- can have spaces
- can start beyond column 1

Improve until the pages:

- can have images

