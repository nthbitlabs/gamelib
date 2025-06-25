export class Time {
    public static now = () => Date.now();
    public static millis = () => Date.now();
    public static secs = () => Math.floor(Date.now() / 1000);
}