$regex = '(?<container>\S+)\s+(?<cpu>\S+)%\s+(?<mem>\S+)\s\/\s\S+\s+(?<memp>\S+)%\s+(?<netio>\S+\s\/\s\S+)\s+(?<blockio>\S+\s\/\s\S+)'

while($true) {
    $docker = docker stats -a --no-stream
    $date = Get-Date -UFormat %s
    $date

    for($i=1; $i -lt $docker.Length; $i++) {
        $docker[$i] -match $regex | Out-Null
        $Matches.Remove(0)
        $fileName = $Matches['container'] + '.csv'
        $Matches.Add('time', $date)
        $out = New-Object psobject -Property $Matches
        $out | Export-Csv $fileName -NoTypeInformation -Append
    }
}