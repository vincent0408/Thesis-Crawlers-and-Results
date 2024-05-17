// game list only for example, simplest way of avoiding CROS
var games = "Just Chatting|Grand Theft Auto V|League of Legends|Fortnite|Palworld|VALORANT|Counter-Strike|Dota 2|EA Sports FC 24|Call of Duty: Warzone|Escape from Tarkov|Apex Legends|TEKKEN 8|Minecraft|Casino|Poppy Playtime|World of Warcraft|Sports|Dead by Daylight|Enshrouded|Slots|Overwatch 2|Tom Clancy's Rainbow Six Siege|Suicide Squad: Kill the Justice League|Call of Duty: Modern Warfare III|Like a Dragon: Infinite Wealth|Music|Teamfight Tactics|PUBG: BATTLEGROUNDS|Virtual Casino|All the Way Down|Genshin Impact|Hearthstone|Special Events|Shave & Stuff VR|ASMR|Dungeonborne|Art|Nightingale|Rocket League".split('|');
function sleep (time) {
    return new Promise((resolve) => setTimeout(resolve, time));
}
async function getRes(){
    var res = [];
    for(let i=0;i<games.length;i++){
        try{
            let search = document.getElementById("Category-Selector");
            search.focus();
            search.value = "";
            document.execCommand('insertText', false, games[i]);
            await sleep(1000);
            poss_results = document.querySelectorAll('p[class="CoreText-sc-1txzju1-0 jkurzn InjectLayout-sc-1i43xsx-0 fKHXti"]');
            for(let j=0;j<poss_results.length;j++){
                // since we got the game_name from Twitch, there should be an exact match s well
                // results show 98% of the time tere is an exact match, 1% failed because of the character '&' in games, 1% of custom categories does not exist anymore
                if(poss_results[j].innerHTML === games[i]){
                    ct = true;
                    poss_results[j].click();
                    await sleep(1000);
                    let ele_lst = document.getElementsByClassName("Layout-sc-1xcs6mc-0 capulb");
                    // maybe different depending on browser and individual behavior
                    if(ele_lst.length == 10){
                        res.push([games[i], "F"]);
                    }
                    else if(ele_lst.length == 11){
                        res.push([games[i], "T"]);
                    }
                    else{
                        console.log(games[i], 'failed');
                    }
                    document.querySelector("button[aria-label='Cancel']").click();
                    break;
                }
            }
        }
        catch(err){
            document.querySelector("button[aria-label='Cancel']").click();
        }
    }
    return res;
}
res = await getRes();
res.map(e => e.join('\\')).join('|');
// "Just Chatting\F|Grand Theft Auto V\T|League of Legends\F|Fortnite\F|Palworld\F|VALORANT\F|Counter-Strike\T|Dota 2\F|EA Sports FC 24\F|Call of Duty: Warzone\T|Escape from Tarkov\T|Apex Legends\F|TEKKEN 8\F|Minecraft\F|Casino\F|Poppy Playtime\F|World of Warcraft\F|Sports\F|Dead by Daylight\T|Enshrouded\F|Slots\T|Overwatch 2\F|Tom Clancy's Rainbow Six Siege\T|Suicide Squad: Kill the Justice League\T|Call of Duty: Modern Warfare III\T|Like a Dragon: Infinite Wealth\T|Music\F|Teamfight Tactics\F|PUBG: BATTLEGROUNDS\F|Virtual Casino\F|All the Way Down\F|Genshin Impact\F|Hearthstone\F|Special Events\F|ASMR\F|Dungeonborne\F|Art\F|Nightingale\F|Rocket League\F"
// sleep time maybe adjusted for stablility, or just set a lower sleep time and retry a few times on unfinished games
